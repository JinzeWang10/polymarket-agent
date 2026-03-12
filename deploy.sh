#!/usr/bin/env bash
set -euo pipefail

# ========== 配置 ==========
REPO_URL="https://github.com/JinzeWang10/polymarket-agent.git"
INSTALL_DIR="$HOME/polymarket-agent"
SERVICE_NAME="polymarket-scanner"
FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/3abab6ae-d8a1-4a0a-b4f9-46abbcbc7ab5"
SCAN_INTERVAL=30  # 分钟

# ========== 安装系统依赖 ==========
echo "[1/6] 安装系统依赖..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-venv python3-pip git > /dev/null

PYTHON=$(command -v python3)
echo "  Python: $($PYTHON --version)"

# ========== 克隆或拉取代码 ==========
echo "[2/6] 同步代码..."
if [ -d "$INSTALL_DIR/.git" ]; then
    cd "$INSTALL_DIR"
    git pull --ff-only
    echo "  已拉取最新代码"
else
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    echo "  已克隆仓库"
fi

# ========== 创建虚拟环境 & 安装依赖 ==========
echo "[3/6] 安装 Python 依赖..."
if [ ! -d ".venv" ]; then
    $PYTHON -m venv .venv
fi
source .venv/bin/activate
pip install -q -e .

# ========== 写入配置 ==========
echo "[4/6] 写入配置..."

# .env
cat > .env <<EOL
FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK}
LOG_LEVEL=INFO
EOL

# 将 scan_interval_minutes 设为指定值
sed -i "s/^scan_interval_minutes:.*/scan_interval_minutes: ${SCAN_INTERVAL}/" config.yaml

echo "  scan_interval_minutes: ${SCAN_INTERVAL}"
echo "  feishu webhook: 已配置"

# ========== 创建 systemd 服务 ==========
echo "[5/6] 配置 systemd 服务..."
VENV_PYTHON="$INSTALL_DIR/.venv/bin/python"

sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=Polymarket Outlier Scanner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=${INSTALL_DIR}
ExecStart=${VENV_PYTHON} -m src.main
Restart=always
RestartSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME} -q

# ========== 启动服务 ==========
echo "[6/6] 启动扫描服务..."
sudo systemctl restart ${SERVICE_NAME}
sleep 2

if systemctl is-active --quiet ${SERVICE_NAME}; then
    echo ""
    echo "========================================"
    echo "  部署成功！扫描服务已启动"
    echo "  扫描间隔: 每 ${SCAN_INTERVAL} 分钟"
    echo "  信号推送: 飞书群"
    echo "========================================"
    echo ""
    echo "常用命令:"
    echo "  查看状态:  sudo systemctl status ${SERVICE_NAME}"
    echo "  实时日志:  journalctl -u ${SERVICE_NAME} -f"
    echo "  重启服务:  sudo systemctl restart ${SERVICE_NAME}"
    echo "  停止服务:  sudo systemctl stop ${SERVICE_NAME}"
else
    echo "启动失败，查看日志:"
    journalctl -u ${SERVICE_NAME} --no-pager -n 20
    exit 1
fi
