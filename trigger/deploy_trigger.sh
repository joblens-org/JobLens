#!/bin/bash
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
#!/bin/bash
# deploy_trigger.sh

# 设置变量
VENV_DIR="venv"
SERVICE_NAME="joblens-trigger"
PROJECT_DIR="$(pwd)"
REQUIREMENTS_FILE="$PROJECT_DIR/requirements.txt"
APP_ENTRY="app:app"  # 假设你的 Flask app 实例在 app.py 中叫 app

# 创建并激活虚拟环境
if [[ ! -d "$VENV_DIR" ]]; then
    echo "Virtual environment not found, creating at $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

# 安装依赖
echo "Installing requirements..."
# 机器只支持v4
"$VENV_DIR/bin/pip" config set global.index-url https://mirrors4.tuna.tsinghua.edu.cn/pypi/web/simple
"$VENV_DIR/bin/pip" install -q --upgrade pip
"$VENV_DIR/bin/pip" install -q -r "$REQUIREMENTS_FILE"

# 创建 systemd 服务文件
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=JobLens Trigger Service
After=network.target

[Service]
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$PROJECT_DIR/$VENV_DIR/bin"
ExecStart=$PROJECT_DIR/$VENV_DIR/bin/gunicorn --config $PROJECT_DIR/gunicorn.conf.py $APP_ENTRY
Restart=always
RestartSec=3
StartLimitInterval=0

[Install]
WantedBy=multi-user.target
EOF

# 重新加载 systemd 并启动服务
echo "Reloading systemd daemon..."
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"
sudo systemctl restart "${SERVICE_NAME}.service"

echo "✅ Deployment complete. Service '${SERVICE_NAME}' is now running."
# sudo systemctl status "${SERVICE_NAME}.service" --no-pager
