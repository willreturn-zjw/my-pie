import json
import subprocess
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

class ChoreographyScheduler:
    def __init__(self, workflow_path):
        self.workflow_path = workflow_path
        with open(workflow_path, 'r') as f:
            self.workflow = json.load(f)
        self.workflow_dir = os.path.dirname(os.path.abspath(workflow_path))

    def run_agent(self, node):
        node_id = node['id']
        wasm_path = os.path.abspath(os.path.join(self.workflow_dir, node['image']))
        payload = {"prompt": node['instruction']}
        
        # 打印启动信息
        print(f"👂 [Standby] {node_id}" if node.get('dependencies') else f"🚀 [Trigger] {node_id}")
        
        cmd = ["pie-cli", "submit", wasm_path, "--", "--input", json.dumps(payload)]
        
        start_t = time.time()
        result_data = {"id": node_id, "status": "Unknown", "duration": 0.0, "log": "", "output": None}

        try:
            # 超时时间设为 100s
            proc = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', env=os.environ.copy(), timeout=100)
            result_data["duration"] = time.time() - start_t
            
            if proc.returncode == 0:
                result_data["status"] = "Success"
                result_data["log"] = f"✅ [Done] {node_id}"
                result_data["output"] = proc.stdout.strip()
            else:
                result_data["status"] = "Error"
                result_data["log"] = f"❌ [Error] {node_id}: {proc.stderr.strip()}"
                
        except subprocess.TimeoutExpired:
            result_data["duration"] = time.time() - start_t
            result_data["status"] = "Timeout"
            result_data["log"] = f"⏰ [Timeout] {node_id}"
            
        except Exception as e:
            result_data["status"] = "Exception"
            result_data["log"] = f"💥 [Exception] {node_id}: {str(e)}"

        return result_data

    def print_stats_table(self, stats, total_wall_time):
        print("\n" + "="*65)
        print(f"{'Node ID':<25} | {'Status':<10} | {'Duration (s)':<12}")
        print("-" * 65)
        for item in sorted(stats, key=lambda x: x['id']):
            print(f"{item['id']:<25} | {item['status']:<10} | {item['duration']:>12.2f}")
        print("-" * 65)
        print(f"{'Total Wall-Time':<25} | {'':<10} | {total_wall_time:>12.2f}")
        print("="*65 + "\n")

    def run(self):
        print(f"=== Starting Choreography: {self.workflow.get('name')} ===")
        nodes = self.workflow['nodes']
        listeners = [n for n in nodes if len(n.get('dependencies', [])) > 0]
        starters = [n for n in nodes if len(n.get('dependencies', [])) == 0]

        all_futures = {}
        execution_stats = []
        overall_start = time.time()

        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            # 1. 启动监听者
            for node in listeners:
                all_futures[executor.submit(self.run_agent, node)] = node['id']
            
            # 等待Wasm冷启动和订阅就绪
            print("⏳ Waiting 10s for listeners to connect...")
            time.sleep(10) 
            
            # 2. 启动触发者
            for node in starters:
                all_futures[executor.submit(self.run_agent, node)] = node['id']
            
            print("\n--- 🔴 Streaming Agent Outputs 🔴 ---\n")
            
            # 3. 实时获取并打印结果
            for future in as_completed(all_futures):
                res = future.result()
                execution_stats.append(res)
                
                # === 重点：这里直接打印每个Agent的执行结果 ===
                print(f"{res['log']} ({res['duration']:.2f}s)")
                if res['output']:
                    # 1. 过滤掉 pie-cli 的系统头信息 (以 Inferlet 开头的行)
                    clean_output = "\n".join([line for line in res['output'].splitlines() if not line.startswith("Inferlet")])
                    # 2. 打印完整内容，不再截断
                    print(f"   >> OUTPUT:\n{clean_output}")
                    print("-" * 30)

        # 4. 最后打印统计表
        self.print_stats_table(execution_stats, time.time() - overall_start)

if __name__ == "__main__":
    scheduler = ChoreographyScheduler("example-apps/The Real-time Newsroom.json")
    scheduler.run()