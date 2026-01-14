import json
import subprocess
import time
import uuid
import os
import sys

class PieScheduler:
    def __init__(self, workflow_path, config_path):
        # 将相对路径转换为绝对路径，避免子进程调用时出错
        self.workflow_path = os.path.abspath(workflow_path)
        self.config_path = os.path.abspath(config_path)
        
        self.results = {} 
        self.run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        # === 路径检查 ===
        print(f"[Scheduler] Init checking...")
        print(f"  - Config:   {self.config_path}")
        print(f"  - Workflow: {self.workflow_path}")

        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
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

        cmd = [
            "pie", "run",
            "--config", self.config_path,
            wasm_path,
            "--", 
            "--input", input_json_str
        ]

        print(f"[Scheduler]     Executing Agent (via Pie Engine)...")
        start_time = time.time()

        # === FIX 1: 设置环境变量减少 Pie 的日志噪音 ===
        env = os.environ.copy()
        env["RUST_LOG"] = "error"  # 只显示错误日志，隐藏 INFO/WARN

        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                encoding='utf-8',
                cwd=os.getcwd(),
                env=env # 传入环境变量
            )
            
            elapsed = time.time() - start_time

            if result.returncode != 0:
                print(f"[Scheduler] ❌ Agent failed with error:\n{result.stderr}")
                return False

            raw_output = result.stdout.strip()

            # === FIX 2: 输出清洗逻辑 ===
            # Pie 的标准输出包含了系统日志和 Agent 结果。
            # 格式通常是: [Logs] ... [Inferlet ID] Completed: \n <CONTENT> \n [Shutdown Logs]
            
            clean_content = raw_output
            
            # 1. 尝试截取 "Completed:" 之后的内容
            if "Completed:" in raw_output:
                # split(..., 1) 只分割第一次出现的位置
                parts = raw_output.split("Completed:", 1)
                if len(parts) > 1:
                    clean_content = parts[1].strip()
            
            # 2. 去除尾部的 Shutdown 日志 (通常包含 "Stopping backend" 或 🔄)
            if "Stopping backend" in clean_content:
                clean_content = clean_content.split("Stopping backend")[0].strip()
            if "🔄" in clean_content: # 去除 emoji 开头的日志
                 clean_content = clean_content.split("🔄")[0].strip()
            # 3. 去除 Llama 模型常见的结束符
            if "<|eot_id|>" in clean_content:
                clean_content = clean_content.replace("<|eot_id|>", "").strip()

            print(f"[Scheduler] ✅ Node {node_id} finished in {elapsed:.2f}s")
            
            # 打印清洗后的结果预览
            preview = clean_content if len(clean_content) < 100 else clean_content[:100] + "..."
            print(f"[Scheduler]    Clean Output: {preview}")

            self.results[node_id] = {
                "content": clean_content, # 存储清洗后的内容
                "status": "success"
            }
            return True

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
    # === 路径配置 (请根据你的实际情况核对) ===
    
    # 1. 配置文件路径
    # 既然 scheduler.py 现在在 /root/pie/ 下，
    # 且 example_config.toml 通常也在 /root/pie/ 下：
    config_file = "../pie/pie/example_config.toml" 

    # 2. 工作流文件路径
    # workflow_demo.json 还在 /root/pie/example-apps/ 下
    workflow_file = "example-apps/workflow_demo.json"

    # 运行
    try:
        scheduler = PieScheduler(workflow_file, config_file)
        scheduler.run()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please verify the file paths in the '__main__' section of scheduler.py")
        sys.exit(1)