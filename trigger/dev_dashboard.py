#!/usr/bin/env python3
"""
Dev server for SSH Dashboard — serves the HTML and provides mock ES data
so you can preview the UI without a running ES cluster.

Usage:
    cd trigger
    python3 dev_dashboard.py
    # Open http://localhost:7592/dashboard/ssh
"""
import os
import time
import random
from flask import Flask, jsonify, request
import yaml

app = Flask(__name__)

# ============================================================
# ES config helpers
# ============================================================
def load_es_config():
    """从 config/config.example.yaml 读取 ES 连接信息"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.example.yaml')
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    es = cfg.get('ES_writer_config', {})
    host = es.get('host', 'http://localhost')
    port = es.get('port', 9200)
    user = es.get('user', '')
    passwd = es.get('passwd', '')
    indexes = es.get('indexs', [])
    base = host if host.startswith('http') else f'http://{host}'
    return {
        'url': f'{base}:{port}',
        'user': user,
        'passwd': passwd,
        'indexes': indexes,
    }

def get_ssh_index():
    """从 ES 配置中找到 SSH session 的索引名"""
    cfg = load_es_config()
    for idx in cfg['indexes']:
        if idx.get('collector_name') == 'ssh_session_collector':
            return idx.get('index_name', 'ssh_session')
    return 'ssh_session'

_es_cfg = load_es_config()
_ssh_index = get_ssh_index()

# ============================================================
# Serve the dashboard HTML
# ============================================================
@app.route('/dashboard/ssh')
def ssh_dashboard():
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'ssh_dashboard.html')
    if os.path.exists(template_path):
        with open(template_path, 'r') as f:
            return f.read()
    return '<html><body><h1>SSH Dashboard template not found</h1></body></html>', 404


# ============================================================
# Mock ES proxy — generates realistic fake process data
# ============================================================
USERS = ['root', 'zhangsan', 'lisi', 'wangwu', 'nobody', 'mysql', 'nginx']
COMMANDS = {
    'bash':   ['/bin/bash', '-bash'],
    'sshd':   ['sshd: root@pts/0', 'sshd: zhangsan@pts/1'],
    'python': ['/usr/bin/python3 -m http.server', 'python3 train.py --epochs 100'],
    'java':   ['java -Xmx4g -jar app.jar', 'java -cp . MyClass'],
    'node':   ['node server.js', 'node index.js'],
    'vim':    ['vim /etc/nginx/nginx.conf', 'vim ~/.bashrc'],
    'htop':   ['htop'],
    'gcc':    ['gcc -O2 -o prog main.c'],
    'mysqld': ['/usr/sbin/mysqld --basedir=/usr'],
    'nginx':  ['nginx: master process', 'nginx: worker process'],
    'sleep':  ['sleep 3600'],
    'top':    ['top'],
    'git':    ['git push origin main'],
    'docker': ['dockerd --storage-driver overlay2'],
    'systemd': ['/usr/lib/systemd/systemd --switched-root --system'],
}

# pid pool: each pid has a fixed parent and command
def build_fake_processes():
    pid = 1
    procs = []
    now = int(time.time() * 1000)

    # system init tree
    systemd = dict(pid=1, ppid=0, user='root', uid=0, comm='systemd',
                   cmdline='/usr/lib/systemd/systemd', state='S', num_threads=1,
                   cpu_percent=0.05, mem_rss_kb=random.randint(8000, 16000),
                   mem_vm_kb=random.randint(40000, 80000), mem_percent=0.1,
                   starttime=100000000, io_read_bytes=random.randint(0, 100000),
                   io_write_bytes=random.randint(0, 50000))
    procs.append(systemd)
    pid += 1

    # sshd master
    sshd = dict(pid=pid, ppid=1, user='root', uid=0, comm='sshd',
                cmdline='/usr/sbin/sshd -D', state='S', num_threads=3,
                cpu_percent=0.1, mem_rss_kb=random.randint(3000, 8000),
                mem_vm_kb=random.randint(10000, 30000), mem_percent=0.05,
                starttime=200000000, io_read_bytes=0, io_write_bytes=0)
    procs.append(sshd)
    sshd_pid = pid
    pid += 1

    # per-user sshd sessions + shells
    for i, user in enumerate(['root', 'zhangsan', 'lisi', 'wangwu']):
        uid = 0 if user == 'root' else (1000 + i)
        # sshd child session
        sess_pid = pid
        procs.append(dict(pid=pid, ppid=sshd_pid, user=user, uid=uid,
                          comm='sshd', cmdline=f'sshd: {user}@pts/{i}',
                          state='S', num_threads=1,
                          cpu_percent=round(random.uniform(0, 0.2), 2),
                          mem_rss_kb=random.randint(2000, 6000),
                          mem_vm_kb=random.randint(8000, 20000),
                          mem_percent=0.03, starttime=300000000 + i * 50000000,
                          io_read_bytes=0, io_write_bytes=0))
        pid += 1
        shell_pid = pid
        procs.append(dict(pid=pid, ppid=sess_pid, user=user, uid=uid,
                          comm='bash', cmdline='-bash',
                          state='S', num_threads=1,
                          cpu_percent=round(random.uniform(0, 0.1), 2),
                          mem_rss_kb=random.randint(2000, 8000),
                          mem_vm_kb=random.randint(6000, 15000),
                          mem_percent=0.04, starttime=300000000 + i * 50000000 + 1000000,
                          io_read_bytes=random.randint(0, 10000),
                          io_write_bytes=random.randint(0, 10000)))
        pid += 1

        # 1-3 child processes per user
        num_children = random.randint(1, 3)
        for j in range(num_children):
            comm = random.choice(list(COMMANDS.keys()))
            cmdline = random.choice(COMMANDS[comm])
            child_pid = pid
            procs.append(dict(pid=pid, ppid=shell_pid, user=user, uid=uid,
                              comm=comm, cmdline=cmdline,
                              state=random.choice('RS'),
                              num_threads=random.randint(1, 8),
                              cpu_percent=round(random.uniform(0, 15), 2),
                              mem_rss_kb=random.randint(500, 500000),
                              mem_vm_kb=random.randint(1000, 2000000),
                              mem_percent=round(random.uniform(0, 5), 2),
                              starttime=400000000 + i * 50000000 + j * 1000000,
                              io_read_bytes=random.randint(0, 5000000),
                              io_write_bytes=random.randint(0, 2000000)))
            pid += 1
            # sometimes grandchild
            if random.random() < 0.3:
                procs.append(dict(pid=pid, ppid=child_pid, user=user, uid=uid,
                                  comm='sleep', cmdline='sleep 600',
                                  state='S', num_threads=1,
                                  cpu_percent=0,
                                  mem_rss_kb=random.randint(200, 2000),
                                  mem_vm_kb=random.randint(1000, 5000),
                                  mem_percent=0.01, starttime=500000000,
                                  io_read_bytes=0, io_write_bytes=0))
                pid += 1

    # system daemons
    for daemon, ppid in [('mysqld', 1), ('nginx', 1), ('docker', 1)]:
        procs.append(dict(pid=pid, ppid=ppid, user=daemon, uid=999,
                          comm=daemon, cmdline=COMMANDS[daemon][0],
                          state='S', num_threads=random.randint(2, 20),
                          cpu_percent=round(random.uniform(0, 5), 2),
                          mem_rss_kb=random.randint(10000, 200000),
                          mem_vm_kb=random.randint(50000, 500000),
                          mem_percent=round(random.uniform(0, 2), 2),
                          starttime=100000000 + pid * 1000,
                          io_read_bytes=random.randint(0, 10000000),
                          io_write_bytes=random.randint(0, 5000000)))
        pid += 1
        # nginx workers
        if daemon == 'nginx':
            for _ in range(2):
                procs.append(dict(pid=pid, ppid=pid - 1, user='nginx', uid=999,
                                  comm='nginx', cmdline='nginx: worker process',
                                  state='S', num_threads=1,
                                  cpu_percent=round(random.uniform(0, 1), 2),
                                  mem_rss_kb=random.randint(5000, 30000),
                                  mem_vm_kb=random.randint(20000, 100000),
                                  mem_percent=0.1, starttime=100000000 + pid * 1000,
                                  io_read_bytes=random.randint(0, 100000),
                                  io_write_bytes=random.randint(0, 1000000)))
                pid += 1

    return procs


@app.route('/dashboard/ssh/api/es', methods=['GET', 'POST'])
def ssh_dashboard_es_proxy():
    """ES 查询代理 — 转发到真实 ES，避免浏览器跨域。"""
    import json as _json
    import requests as req
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if request.method == 'POST':
        body = request.get_json(silent=True) or {}
    else:
        body = _json.loads(request.args.get('body', '{}'))

    # 构造 ES 搜索 URL：用通配符匹配日期索引
    es_url = f"{_es_cfg['url']}/{_ssh_index}_*/_search"

    auth = None
    if _es_cfg['user']:
        auth = (_es_cfg['user'], _es_cfg['passwd'])

    try:
        r = req.post(es_url, json=body, auth=auth, timeout=15, verify=False)
        if r.status_code >= 400:
            return jsonify({'error': f'ES returned {r.status_code}', 'detail': r.text[:500]}), 502
        return jsonify(r.json())
    except req.exceptions.Timeout:
        return jsonify({'error': 'ES query timeout'}), 504
    except req.exceptions.ConnectionError as e:
        return jsonify({'error': f'Cannot connect to ES: {e}'}), 502
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("  SSH Dashboard Dev Server")
    print("  Open: http://localhost:7592/dashboard/ssh")
    print("=" * 60)
    app.run(host='0.0.0.0', port=7592, debug=True)