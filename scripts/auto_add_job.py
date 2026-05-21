#!/usr/bin/env python3
#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#!/usr/bin/env python3
# auto_add_job.py
# usage: ssh host "cd /home/cc/wzycc/JobLens/scripts/ && python3 ./auto_add_job.py slotx {jobID}" //从ccopt执行

import os, sys, json, time, subprocess, argparse, re, datetime

JOB_LENS_BIN = '/home/cc/wzycc/JobLens/bin/JobLens -m service -c /home/cc/wzycc/JobLens/config/config.yaml'
FIFO_PATH    = '/var/JobLens/job_adder_fifo'
LOG_DIR      = '/var/JobLens'
JOBLENS_FOREGROUND = True

# ----------------- 工具函数 -----------------
def log(msg):
    """统一带时间戳打印，方便后台排查。"""
    ts = datetime.datetime.now().strftime('%F %T')
    print(f"[{ts}] {msg}")

def run(cmd, check=True):
    cp = subprocess.run(cmd, shell=True, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and cp.returncode != 0:
        raise RuntimeError(f"命令失败: {cmd}\n{cp.stdout}")
    return cp.stdout.strip()

def ensure_joblens_running():
    if run('pgrep -f JobLens', check=False):
        log("JobLens 已在运行。")
        return

    log("JobLens 未检测到，正在启动 …")
    if JOBLENS_FOREGROUND:
        # 前台启动，输出直接打到当前终端
        subprocess.Popen(f'{JOB_LENS_BIN}', shell=True,
                         stdin=subprocess.DEVNULL)
        time.sleep(1.5)          # 给一点初始化时间
    else:
        # 原后台方案
        run(f'nohup {JOB_LENS_BIN} > /dev/null 2>&1 &')
        time.sleep(1.5)

def find_pids_by_slot(slot: str):
    """返回 slot 对应 condor_starter 下、排除自身及 {} 线程后的真实子进程 PID 列表。"""
    if not slot.startswith('slot'):
        raise ValueError("slot 名必须以 'slot' 开头")
    num = slot[4:]
    ps_out = run(f"ps -ax -o pid,cmd --no-headers | "
                 f"grep -E 'condor_starter.*[s]lot{num}'")
    if not ps_out:
        raise RuntimeError(f"未找到 {slot} 对应的 condor_starter")
    starter_pid = ps_out.split()[0]
    log(f"=== 进程树：{slot}  starter={starter_pid} ===")

    # 1. 整棵树所有 PID
    tree = run(f"pstree -pT {starter_pid}")
    print(tree)
    all_pids = {int(x) for x in re.findall(r'\((\d+)\)', tree)}

    # 2. PID→comm 映射
    pid_comm = {}
    for line in run("ps -eo pid,comm --no-headers").splitlines():
        if line:
            pid, comm = line.strip().split(None, 1)
            pid_comm[int(pid)] = comm

    print(sorted(all_pids))
    return sorted(all_pids)

def write_fifo(job_id: int, pids: list):
    obj = {
        "opt": "add",
        "type": "job.common",
        "JobID": job_id,
        "JobPIDs": pids,
        "Lens": ["cpumem_collector", "io_collector", "net_collector"]
    }
    with open(FIFO_PATH, 'w') as f:
        f.write(json.dumps(obj))
        f.flush()
    log(f"已写入 FIFO：JobID={job_id}，PID 列表={pids}")

def work(slot: str, job_id: int):
    """真正干活的流程，可被前台或后台调用。"""
    os.makedirs(LOG_DIR, exist_ok=True)
    ensure_joblens_running()
    pids = find_pids_by_slot(slot)
    if not pids:
        log("未找到符合条件的子进程，放弃添加。")
        sys.exit(2)
    write_fifo(job_id, pids)
    log("Done.")

# ----------------- 参数解析 -----------------
def main():
    parser = argparse.ArgumentParser(description='一键添加作业到 JobLens（支持前台/后台）')
    parser.add_argument('slot', help='槽位名，如 slot123')
    parser.add_argument('job_id', type=int, help='要绑定的 JobID')
    args = parser.parse_args()

    work(args.slot, args.job_id)

if __name__ == '__main__':
    main()