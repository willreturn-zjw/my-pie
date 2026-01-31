import json
import subprocess
import time
import uuid
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# 用于统计信息的简单类
class TaskStats:
    def __init__(self, node_id):
        self.node_id = node_id
        self.start_time = None
        self.end_time = None
        self.duration = 0.0
        self.status = "Pending"

    def start(self):
        self.start_time = datetime.now()
        
    def finish(self, status="Success"):
        self.end_time = datetime.now()
        if self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = status

    def __str__(self):
        s_str = self.start_time.strftime("%H:%M:%S") if self.start_time else "N/A"
        e_str = self.end_time.strftime("%H:%M:%S") if self.end_time else "N/A"
        return f"| {self.node_id:<20} | {s_str:<10} | {e_str:<10} | {self.duration:>8.2f}s | {self.status:<10} |"

class PieScheduler:
    def __init__(self, workflow_path):
        self.workflow_path = os.path.abspath(workflow_path)
        self.workflow_dir = os.path.dirname(self.workflow_path)
        self.results = {} 
        self.stats_map = {} # 存储统计信息
        self.run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        print(f"[Scheduler] Init checking...")
        print(f"  - Workflow: {self.workflow_path}")
        print(f"  - Run ID:   {self.run_id}")

        if not os.path.exists(self.workflow_path):
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
            
        self.workflow = self._load_workflow()
        self.node_map = {n['id']: n for n in self.workflow['nodes']}
        
        # 初始化统计对象
        for nid in self.node_map:
            self.stats_map[nid] = TaskStats(nid)

    def _load_workflow(self):
        with open(self.workflow_path, 'r') as f:
            return json.load(f)

    def _get_task_id(self, node_id):
        return f"{self.run_id}_{node_id}"

    def run_node(self, node): 
        node_id = node['id']
        stats = self.stats_map[node_id] # 获取对应的统计对象
        
        raw_image_path = node['image']
        wasm_path = os.path.join(self.workflow_dir, raw_image_path)
        wasm_path = os.path.abspath(wasm_path)

        # === 计时开始 ===
        stats.start()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Scheduler] ➤ [Start] {node_id}")
        
        if not os.path.exists(wasm_path):
            print(f"[Error] Wasm file not found at: {wasm_path}")
            stats.finish("Error")
            return False, ""

        try:
            dependencies = node.get("dependencies", [])
            parent_task_ids = [self._get_task_id(dep_id) for dep_id in dependencies]
            current_task_id = self._get_task_id(node_id)

            input_payload = {
                "task_id": current_task_id,
                "parent_task_ids": parent_task_ids, 
                "prompt": node.get("instruction", "")
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

            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding='utf-8',
                cwd=os.getcwd(), env=env
            )

            if result.returncode != 0:
                print(f"[Scheduler] ❌ Node {node_id} failed:\n{result.stderr}")
                stats.finish("Failed")
                return False, result.stderr

            raw_output = result.stdout.strip()
            
            # === 计时结束 ===
            stats.finish("Success")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Scheduler] ✅ [Finish] {node_id} ({stats.duration:.2f}s)")
            
            return True, raw_output

        except Exception as e:
            print(f"[Scheduler] System Error in {node_id}: {e}")
            stats.finish("Exception")
            return False, str(e)

    def print_summary(self):
        """打印漂亮的汇总表格"""
        print("\n" + "="*80)
        print(f"Workflow Execution Summary: {self.workflow.get('name', 'Unknown')}")
        print(f"Run ID: {self.run_id}")
        print("-" * 80)
        print(f"| {'Node ID':<20} | {'Start':<10} | {'End':<10} | {'Duration':<9} | {'Status':<10} |")
        print("-" * 80)
        
        # 按开始时间排序
        sorted_stats = sorted(
            [s for s in self.stats_map.values() if s.start_time], 
            key=lambda x: x.start_time
        )
        # 把未执行的也加在后面
        sorted_stats.extend([s for s in self.stats_map.values() if not s.start_time])

        start_times = []
        end_times = []

        for stat in sorted_stats:
            print(stat)
            if stat.start_time: start_times.append(stat.start_time)
            if stat.end_time: end_times.append(stat.end_time)
            
        print("-" * 80)
        if start_times and end_times:
            global_start = min(start_times)
            global_end = max(end_times)
            total_wall_time = (global_end - global_start).total_seconds()
            print(f"Total Wall-clock Time: {total_wall_time:.2f}s")
        else:
            print("Total Wall-clock Time: N/A")
        print("="*80 + "\n")

    def run(self):
        print(f"=== Starting Workflow: {self.workflow.get('name', 'Untitled')} ===")
        
        pending_ids = set(self.node_map.keys())
        completed_ids = set()
        running_ids = set()
        max_parallel = 4 
        
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {}
            try:
                while pending_ids or futures:
                    ready_nodes = []
                    for nid in list(pending_ids):
                        if nid in running_ids: continue
                        node = self.node_map[nid]
                        deps = node.get("dependencies", [])
                        if all(d in completed_ids for d in deps):
                            ready_nodes.append(node)

                    for node in ready_nodes:
                        nid = node['id']
                        print(f"[Scheduler] Submitting {nid}...")
                        future = executor.submit(self.run_node, node)
                        futures[future] = nid
                        running_ids.add(nid)

                    if not futures and not ready_nodes and pending_ids:
                        remaining = pending_ids - running_ids
                        if remaining:
                            print(f"[Scheduler] ❌ Deadlock detected! Remaining: {remaining}")
                            break

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
                                    return 
                            except Exception as e:
                                print(f"[Scheduler] 💥 Exception: {e}")
                                return
                print(f"\n=== Workflow Completed Successfully! ===")

                print(f"Final Results:")

                # 按照简单的依赖顺序打印结果，或者直接按 ID 打印

                for nid, res in self.results.items():

                    print(f"\n>>>>> Node: [{nid}] <<<<<")

                    print(res['content'])

                    print("-" * 40)
            finally:
                # 无论成功还是失败，最后都打印表格
                self.print_summary()

if __name__ == "__main__":
    workflow_file = "example-apps/The Rashomon Mystery.json" 
    try:
        scheduler = PieScheduler(workflow_file)
        scheduler.run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)