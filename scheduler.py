import json
import subprocess
import time
import uuid
import os
import sys

class PieScheduler:
    def __init__(self, workflow_path):
        # 3.1阶段：不再需要 config_path，因为配置由后台 pie serve 管理
        self.workflow_path = os.path.abspath(workflow_path)
        self.results = {} 
        self.run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        print(f"[Scheduler] Init checking...")
        print(f"  - Workflow: {self.workflow_path}")
        print(f"  - Mode:     Client/Server (Connecting to pie serve)")

        if not os.path.exists(self.workflow_path):
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
            
        self.workflow = self._load_workflow()

    def _load_workflow(self):
        with open(self.workflow_path, 'r') as f:
            return json.load(f)

    def _get_upstream_data(self, dependencies):
        upstream_data = {}
        for dep_id in dependencies:
            if dep_id in self.results:
                upstream_data[dep_id] = self.results[dep_id]['content']
            else:
                raise Exception(f"Dependency {dep_id} not executed yet!")
        return upstream_data

    def run_node(self, node):
        node_id = node['id']
        raw_image_path = node['image']
        
        workflow_dir = os.path.dirname(self.workflow_path)
        wasm_path = os.path.join(workflow_dir, raw_image_path)
        wasm_path = os.path.abspath(wasm_path)

        print(f"\n[Scheduler] ➤ Scheduling Node: {node_id}")
        
        if not os.path.exists(wasm_path):
            print(f"[Error] Wasm file not found at: {wasm_path}")
            return False

        input_payload = {
            "run_id": self.run_id,
            "node_id": node_id,
            "input_context": node.get("config", {}),
            "upstream_results": self._get_upstream_data(node.get("dependencies", []))
        }

        input_json_str = json.dumps(input_payload)

        # === 核心修改：使用 pie-cli submit ===
        # pie-cli submit <wasm> -- --input <json>
        # 注意：这里假设 pie-cli 在 PATH 中，或者在 target/release/pie-cli
        # 为了稳妥，我们尝试使用 'pie-cli' 命令，如果不行请修改为绝对路径
        cmd = [
            "pie-cli", "submit",
            wasm_path,
            "--", 
            "--input", input_json_str
        ]

        print(f"[Scheduler]     Submitting Agent to Engine (via pie-cli)...")
        start_time = time.time()

        # 同样设置 ENV 减少客户端日志干扰
        env = os.environ.copy()
        env["RUST_LOG"] = "error"

        try:
            # pie-cli submit 会连接 localhost:8080 并流式输出结果
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                encoding='utf-8',
                cwd=os.getcwd(),
                env=env
            )
            
            elapsed = time.time() - start_time

            if result.returncode != 0:
                print(f"[Scheduler] ❌ Agent submission failed:\n{result.stderr}")
                # 常见错误：Engine 没启动
                if "Connection refused" in result.stderr:
                    print("[Scheduler] 💡 Tip: Did you run 'pie serve' in another terminal?")
                return False

            raw_output = result.stdout.strip()

            # === 输出清洗逻辑 (保持 Step 2 的逻辑) ===
            clean_content = raw_output
            
            # pie-cli 的输出可能包含 "Inferlet launched with ID: ..." 等头部信息
            # 我们的 Agent 输出通常在最后。
            # 为了简单适配，我们尝试寻找 Agent 的特征输出
            
            # 策略：如果 raw_output 包含我们 KVS 写入的 success 标记或者直接取最后一段
            # 这里暂时沿用之前的清洗逻辑
            if "Completed:" in raw_output:
                parts = raw_output.split("Completed:", 1)
                if len(parts) > 1:
                    clean_content = parts[1].strip()
            
            if "Stopping backend" in clean_content:
                clean_content = clean_content.split("Stopping backend")[0].strip()
            if "🔄" in clean_content:
                 clean_content = clean_content.split("🔄")[0].strip()
            if "<|eot_id|>" in clean_content:
                clean_content = clean_content.replace("<|eot_id|>", "").strip()

            # 去除 pie-cli 可能特有的头部日志
            lines = clean_content.split('\n')
            # 简单的 heuristic: 如果第一行包含 "Inferlet launched", 去掉它
            if lines and "Inferlet launched" in lines[0]:
                clean_content = "\n".join(lines[1:]).strip()

            print(f"[Scheduler] ✅ Node {node_id} finished in {elapsed:.2f}s")
            preview = clean_content if len(clean_content) < 100 else clean_content[:100] + "..."
            print(f"[Scheduler]    Clean Output: {preview}")

            self.results[node_id] = {
                "content": clean_content,
                "status": "success"
            }
            return True

        except FileNotFoundError:
            print("[Scheduler] ❌ Error: 'pie-cli' command not found. Please add it to PATH or edit scheduler.py.")
            return False
        except Exception as e:
            print(f"[Scheduler] System Error: {e}")
            return False

    def run(self):
        print(f"=== Starting Workflow: {self.workflow['name']} (ID: {self.run_id}) ===")
        
        pending_nodes = {n['id']: n for n in self.workflow['nodes']}
        completed_nodes = set()

        while pending_nodes:
            progress_made = False
            ready_nodes = []

            for node_id, node in pending_nodes.items():
                deps = node.get("dependencies", [])
                if all(d in completed_nodes for d in deps):
                    ready_nodes.append(node)

            if not ready_nodes:
                print("[Scheduler] ❌ Deadlock detected!")
                break

            for node in ready_nodes:
                success = self.run_node(node)
                if success:
                    completed_nodes.add(node['id'])
                    del pending_nodes[node['id']]
                    progress_made = True
                else:
                    print(f"[Scheduler] ❌ Workflow aborted due to failure in {node['id']}")
                    return

            if not progress_made:
                break

        print(f"\n=== Workflow Completed Successfully! ===")
        print(f"Final Results:")
        for nid, res in self.results.items():
            print(f"[{nid}]: {res['content']}")

if __name__ == "__main__":
    workflow_file = "example-apps/workflow_demo.json"
    
    try:
        # Step 3.1: 只需要传入 workflow 文件路径
        scheduler = PieScheduler(workflow_file)
        scheduler.run()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)