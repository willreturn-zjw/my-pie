import json
import subprocess
import time
import uuid
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

class PieScheduler:
    def __init__(self, workflow_path):
        self.workflow_path = os.path.abspath(workflow_path)
        self.results = {} 
        self.run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        print(f"[Scheduler] Init checking...")
        print(f"  - Workflow: {self.workflow_path}")
        print(f"  - Mode:     Parallel Execution (ThreadPool)")

        if not os.path.exists(self.workflow_path):
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
            
        self.workflow = self._load_workflow()

    def _load_workflow(self):
        with open(self.workflow_path, 'r') as f:
            return json.load(f)

    def _get_upstream_data(self, dependencies):
        upstream_data = {}
        for dep_id in dependencies:
            # 这里的读取需要注意线程安全，但在 Python GIL 下字典读取通常是原子性的，
            # 且我们的逻辑保证了只有依赖完成后才会读取，所以是安全的。
            if dep_id in self.results:
                upstream_data[dep_id] = self.results[dep_id]['content']
            else:
                raise Exception(f"Dependency {dep_id} not executed yet!")
        return upstream_data

    def run_node(self, node):
        """
        这个函数将在线程池中运行。它是阻塞的（执行 subprocess），
        但不会阻塞主调度循环。
        """
        node_id = node['id']
        raw_image_path = node['image']
        
        workflow_dir = os.path.dirname(self.workflow_path)
        wasm_path = os.path.join(workflow_dir, raw_image_path)
        wasm_path = os.path.abspath(wasm_path)

        # 打印时带上线程信息或仅仅是前缀，证明并行
        start_ts = datetime.now().strftime("%H:%M:%S.%f")[:12]
        print(f"[{start_ts}] [Scheduler] ➤ [Start] {node_id}") # 带时间戳
        
        if not os.path.exists(wasm_path):
            print(f"[Error] Wasm file not found at: {wasm_path}")
            return False

        try:
            input_payload = {
                "run_id": self.run_id,
                "node_id": node_id,
                "input_context": node.get("config", {}),
                "upstream_results": self._get_upstream_data(node.get("dependencies", []))
            }
            input_json_str = json.dumps(input_payload)

            cmd = [
                "pie-cli", "submit",
                wasm_path,
                "--", 
                "--input", input_json_str
            ]

            env = os.environ.copy()
            env["RUST_LOG"] = "error"

            start_time = time.time()
            
            # 这里依然是阻塞调用，但它是在子线程里阻塞，不会卡住主线程
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
                print(f"[Scheduler] ❌ Node {node_id} failed:\n{result.stderr}")
                return False

            raw_output = result.stdout.strip()
            
            # === 输出清洗逻辑 ===
            clean_content = raw_output
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
            lines = clean_content.split('\n')
            if lines and "Inferlet launched" in lines[0]:
                clean_content = "\n".join(lines[1:]).strip()

            end_ts = datetime.now().strftime("%H:%M:%S.%f")[:12]
            print(f"[{end_ts}] [Scheduler] ✅ [Finish] {node_id} ({elapsed:.2f}s)")
            
            # 将结果写入共享字典
            self.results[node_id] = {
                "content": clean_content,
                "status": "success"
            }
            return True

        except Exception as e:
            print(f"[Scheduler] System Error in {node_id}: {e}")
            return False

    def run(self):
        print(f"=== Starting Workflow: {self.workflow['name']} (ID: {self.run_id}) ===")
        
        all_nodes = {n['id']: n for n in self.workflow['nodes']}
        pending_ids = set(all_nodes.keys())
        completed_ids = set()
        running_ids = set() # 记录正在运行的节点

        # 创建线程池，最大并发数设为 4（可根据演示需要调整）
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {} # 映射：Future对象 -> node_id

            # === 事件驱动循环 ===
            while pending_ids or futures:
                # 1. 扫描可运行的节点
                # 条件：在等待列表 + 依赖全部完成 + 没在运行
                ready_nodes = []
                for nid in list(pending_ids): # 用 list 复制一份以防遍历时修改
                    if nid in running_ids:
                        continue
                        
                    node = all_nodes[nid]
                    deps = node.get("dependencies", [])
                    if all(d in completed_ids for d in deps):
                        ready_nodes.append(node)

                # 2. 发射任务 (Launch)
                for node in ready_nodes:
                    nid = node['id']
                    # 提交给线程池，非阻塞
                    future = executor.submit(self.run_node, node)
                    futures[future] = nid
                    
                    # 标记状态
                    running_ids.add(nid)
                    # 注意：此时不能从 pending_ids 删除，要等真正完成才删，
                    # 或者现在删也行，但为了逻辑清晰，我们在完成时处理 pending

                if not futures and not ready_nodes:
                    print("[Scheduler] ❌ Deadlock or no nodes ready!")
                    break

                # 3. 等待任意一个任务完成 (Wait for Event)
                # return_when=FIRST_COMPLETED 是实现流水线并行的关键
                if futures:
                    done, not_done = wait(futures.keys(), return_when=FIRST_COMPLETED)
                    
                    # 处理完成的任务
                    for f in done:
                        nid = futures.pop(f) # 从监控列表中移除
                        try:
                            success = f.result() # 获取返回值
                            if success:
                                completed_ids.add(nid)
                                pending_ids.remove(nid) # 彻底完工
                            else:
                                print(f"[Scheduler] ❌ Workflow aborted due to failure in {nid}")
                                return # 简单起见，有一个失败就终止
                        except Exception as e:
                            print(f"[Scheduler] 💥 Exception in worker: {e}")
                            return
                        
                        running_ids.remove(nid)
            
        print(f"\n=== Workflow Completed Successfully! ===")
        print(f"Final Results:")
        for nid, res in self.results.items():
            # [修改] 打印完整内容，不再截断
            print(f"\n>>>>> Node: [{nid}] <<<<<")
            print(res['content'])
            print("-" * 40)

if __name__ == "__main__":
    workflow_file = "example-apps/workflow_demo.json"
    try:
        scheduler = PieScheduler(workflow_file)
        scheduler.run()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)