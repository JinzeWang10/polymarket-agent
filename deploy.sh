#!/usr/bin/env bash
set -euo pipefail

# ========== 配置 ==========
REPO_URL="https://github.com/JinzeWang10/polymarket-agent.git"
INSTALL_DIR="$HOME/polymarket-agent"
MAIN_SERVICE="polymarket-scanner"   # src.main: outlier 10min + worldcup 5min + live-lag 60s
PENNY_SERVICE="polymarket-penny"    # src.penny_main: NBA + WorldCup live windows
FEISHU_WEBHOOK="https://open.feishu.cn/open-apis/bot/v2/hook/3abab6ae-d8a1-4a0a-b4f9-46abbcbc7ab5"

# ========== 安装系统依赖 ==========
echo "[1/6] 安装系统依赖..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-venv python3-pip git curl > /dev/null

# 项目要求 Python >= 3.11（Ubuntu 22.04 默认是 3.10，需要装新版）
PYTHON=""
for cand in python3.13 python3.12 python3.11 python3; do
    if command -v "$cand" > /dev/null; then
        if "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
            PYTHON=$(command -v "$cand")
            break
        fi
    fi
done
if [ -z "$PYTHON" ]; then
    echo "错误: 找不到 Python >= 3.11。Ubuntu 22.04 请先执行:"
    echo "  sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install python3.11 python3.11-venv"
    exit 1
fi
echo "  Python: $($PYTHON --version) ($PYTHON)"

# ========== 检查 Polymarket API 连通性 ==========
echo "[2/6] 检查 Polymarket API 连通性..."
GAMMA_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
    "https://gamma-api.polymarket.com/events?limit=1" || echo "000")
if [ "$GAMMA_CODE" != "200" ]; then
    echo "错误: 无法访问 gamma-api.polymarket.com (HTTP $GAMMA_CODE)"
    echo "  此服务器网络无法直连 Polymarket，请换香港/新加坡/日本节点"
    exit 1
fi
echo "  gamma-api: OK"

# ========== 克隆或拉取代码 ==========
echo "[3/6] 同步代码..."
if [ -d "$INSTALL_DIR/.git" ]; then
    cd "$INSTALL_DIR"
    # 丢弃服务器上对受 git 管理文件的改动（配置请改本地仓库后 push）
    git checkout -- .
    git pull --ff-only
    echo "  已拉取最新代码: $(git log --oneline -1)"
else
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    echo "  已克隆仓库: $(git log --oneline -1)"
fi

# ========== 创建虚拟环境 & 安装依赖 ==========
echo "[4/6] 安装 Python 依赖..."
if [ ! -d ".venv" ]; then
    $PYTHON -m venv .venv
fi
.venv/bin/pip install -q -e .

# ========== 写入 .env ==========
echo "[5/6] 写入 .env..."
cat > .env <<EOL
FEISHU_WEBHOOK_URL=${FEISHU_WEBHOOK}
LOG_LEVEL=INFO
EOL
echo "  feishu webhook: 已配置"

# ========== 创建并启动 systemd 服务 ==========
echo "[6/6] 配置 systemd 服务..."
VENV_PYTHON="$INSTALL_DIR/.venv/bin/python"

write_service() {
    local name="$1" desc="$2" module="$3"
    sudo tee "/etc/systemd/system/${name}.service" > /dev/null <<EOF
[Unit]
Description=${desc}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=${INSTALL_DIR}
ExecStart=${VENV_PYTHON} -m ${module}
Restart=always
RestartSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
}

write_service "$MAIN_SERVICE" "Polymarket scanner (outlier + worldcup + live-lag)" "src.main"
write_service "$PENNY_SERVICE" "Polymarket penny scanner (NBA + WorldCup windows)" "src.penny_main"

sudo systemctl daemon-reload
sudo systemctl enable "$MAIN_SERVICE" "$PENNY_SERVICE" -q
sudo systemctl restart "$MAIN_SERVICE" "$PENNY_SERVICE"
sleep 3

FAILED=0
for svc in "$MAIN_SERVICE" "$PENNY_SERVICE"; do
    if systemctl is-active --quiet "$svc"; then
        echo "  $svc: 运行中"
    else
        echo "  $svc: 启动失败！最近日志:"
        journalctl -u "$svc" --no-pager -n 20
        FAILED=1
    fi
done
[ "$FAILED" -eq 1 ] && exit 1

echo ""
echo "========================================"
echo "  部署成功！两个扫描服务已启动"
echo "  $MAIN_SERVICE: outlier 10min + 世界杯结构/价值 5min + 滚球滞后 60s"
echo "  $PENNY_SERVICE: penny picking (NBA 8-13点 / 世界杯 0-13点, 北京时间)"
echo "========================================"
echo ""
echo "常用命令:"
echo "  实时日志:  journalctl -u $MAIN_SERVICE -f"
echo "             journalctl -u $PENNY_SERVICE -f"
echo "  查看状态:  systemctl status $MAIN_SERVICE $PENNY_SERVICE"
echo "  重启服务:  sudo systemctl restart $MAIN_SERVICE $PENNY_SERVICE"
echo "  更新部署:  重新运行本脚本即可 (git pull + 重启)"
