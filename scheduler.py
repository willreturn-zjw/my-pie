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
        self.workflow_dir = os.path.dirname(self.workflow_path)
        self.results = {} 
        self.run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        print(f"[Scheduler] Init checking...")
        print(f"  - Workflow: {self.workflow_path}")
        print(f"  - Run ID:   {self.run_id}")

        if not os.path.exists(self.workflow_path):
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
            
        self.workflow = self._load_workflow()
        self.node_map = {n['id']: n for n in self.workflow['nodes']}

    def _load_workflow(self):
        with open(self.workflow_path, 'r') as f:
            return json.load(f)

    def _get_task_id(self, node_id):
        """生成全局唯一的 Task ID"""
        return f"{self.run_id}_{node_id}"

    def run_node(self, node): 
        node_id = node['id']
        raw_image_path = node['image']
        
        # 1. 路径解析 (基于 workflow 文件所在目录)
        wasm_path = os.path.join(self.workflow_dir, raw_image_path)
        wasm_path = os.path.abspath(wasm_path)

        start_ts = datetime.now().strftime("%H:%M:%S.%f")[:12]
        print(f"[{start_ts}] [Scheduler] ➤ [Start] {node_id}")
        
        if not os.path.exists(wasm_path):
            print(f"[Error] Wasm file not found at: {wasm_path}")
            return False, ""

        try:
            # 2. 解析依赖关系，构造父任务 ID 列表
            # 这是支持 Merge 节点的关键：传入所有父节点的 Task ID
            dependencies = node.get("dependencies", [])
            parent_task_ids = [self._get_task_id(dep_id) for dep_id in dependencies]
            
            current_task_id = self._get_task_id(node_id)

            # 3. 构造 Payload (极简协议)
            # 不再包含 mode, max_tokens, temperature 等业务参数
            # 只包含：我是谁(task_id)，我爸是谁(parent_ids)，我要干嘛(prompt)
            input_payload = {
                "task_id": current_task_id,
                "parent_task_ids": parent_task_ids, 
                "prompt": node.get("instruction", "")
            }
            
            input_json_str = json.dumps(input_payload)

            # 4. 调用 Pie 引擎
            cmd = [
                "pie-cli", "submit",
                wasm_path,
                "--", 
                "--input", input_json_str
            ]

            # 环境变量清理
            env = os.environ.copy()
            env["RUST_LOG"] = "error" # 减少底层日志噪音

            start_time = time.time()
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding='utf-8',
                cwd=os.getcwd(), env=env
            )
            elapsed = time.time() - start_time

            if result.returncode != 0:
                print(f"[Scheduler] ❌ Node {node_id} failed:\n{result.stderr}")
                return False, result.stderr

            raw_output = result.stdout.strip()
            
            end_ts = datetime.now().strftime("%H:%M:%S.%f")[:12]
            print(f"[{end_ts}] [Scheduler] ✅ [Finish] {node_id} ({elapsed:.2f}s)")
            
            return True, raw_output

        except Exception as e:
            print(f"[Scheduler] System Error in {node_id}: {e}")
            import traceback
            traceback.print_exc()
            return False, str(e)

    def run(self):
        print(f"=== Starting Workflow: {self.workflow.get('name', 'Untitled')} ===")
        
        pending_ids = set(self.node_map.keys())
        completed_ids = set()
        running_ids = set()
        
        # 拓扑排序/依赖检查循环
        # max_workers 可以根据显存大小调整
        max_parallel = 4
        
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {}

            while pending_ids or futures:
                # A. 扫描所有可以执行的节点 (Ready Nodes)
                # 条件：所有依赖都在 completed_ids 中，且自身没在运行
                ready_nodes = []
                for nid in list(pending_ids):
                    if nid in running_ids: continue
                    
                    node = self.node_map[nid]
                    deps = node.get("dependencies", [])
                    
                    if all(d in completed_ids for d in deps):
                        ready_nodes.append(node)

                # B. 提交任务到线程池
                for node in ready_nodes:
                    nid = node['id']
                    print(f"[Scheduler] Submitting {nid}...")
                    future = executor.submit(self.run_node, node)
                    futures[future] = nid
                    running_ids.add(nid)
                    # 注意：pending_ids 在这里不能删，要等做完才删

                if not futures and not ready_nodes and pending_ids:
                    remaining = pending_ids - running_ids
                    if remaining:
                        print(f"[Scheduler] ❌ Deadlock detected! Remaining nodes waiting for deps: {remaining}")
                        # 打印一下具体的依赖缺失情况，方便调试
                        for rid in remaining:
                            print(f"  - {rid} needs: {self.node_map[rid].get('dependencies')}")
                        break

                # C. 事件循环：等待任意一个任务完成
                if futures:
                    done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
                    for f in done:
                        nid = futures.pop(f)
                        running_ids.remove(nid)
                        
                        try:
                            success, content = f.result()
                            if success:
                                completed_ids.add(nid)
                                pending_ids.remove(nid)
                                self.results[nid] = {"content": content, "status": "success"}
                            else:
                                print(f"[Scheduler] ❌ Aborting workflow due to failure in {nid}")
                                # 遇到错误是否继续？这里选择终止
                                return 
                        except Exception as e:
                            print(f"[Scheduler] 💥 Exception in worker thread: {e}")
                            return
            
        print(f"\n=== Workflow Completed Successfully! ===")
        print(f"Final Results:")
        # 按照简单的依赖顺序打印结果，或者直接按 ID 打印
        for nid, res in self.results.items():
            print(f"\n>>>>> Node: [{nid}] <<<<<")
            print(res['content'])
            print("-" * 40)

if __name__ == "__main__":
    workflow_file = "example-apps/workflow.json" 
    try:
        scheduler = PieScheduler(workflow_file)
        scheduler.run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)