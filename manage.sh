#!/usr/bin/env bash
set -euo pipefail

DEFAULT_SERVER_DIR="/home/admin/GRID_TRADING"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${BASE_DIR:-}" ]]; then
  BASE_DIR="$BASE_DIR"
elif [[ -d "$DEFAULT_SERVER_DIR" ]]; then
  BASE_DIR="$DEFAULT_SERVER_DIR"
else
  BASE_DIR="$LOCAL_DIR"
fi
VENV_PATH="$BASE_DIR/.venv"
PYTHON_BIN="$VENV_PATH/bin/python3"
ENTRY_SCRIPT="$BASE_DIR/bot.py"
CONFIG_PY="$BASE_DIR/configs.py"

usage() {
  cat <<'EOF'
用法:
  ./manage.sh status
  ./manage.sh start <profile|all>
  ./manage.sh stop <profile|all>
  ./manage.sh restart <profile|all>
  ./manage.sh attach <profile>
  ./manage.sh logs <profile>
  ./manage.sh list

示例:
  ./manage.sh start sto_short_2d66
  ./manage.sh stop sto_short_2d66
  ./manage.sh restart sto_short_2d66
  ./manage.sh attach sto_short_2d66
  ./manage.sh logs sto_short_2d66
EOF
}

echo "[INFO] BASE_DIR=$BASE_DIR"

list_profiles() {
  if [[ -f "$CONFIG_PY" ]]; then
    "$PYTHON_BIN" - <<PY
import importlib.util
spec = importlib.util.spec_from_file_location("runtime_configs", "$CONFIG_PY")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
configs = getattr(mod, "CONFIGS", {})
for k in sorted(configs.keys()):
    print(k)
PY
  else
    echo "[WARN] 未找到 configs.py，请手动管理 profile"
  fi
}

session_exists() {
  tmux has-session -t "$1" 2>/dev/null
}

start_one() {
  local profile="$1"

  if session_exists "$profile"; then
    echo "[INFO] $profile 已在运行"
    return
  fi

  echo "[INFO] 启动 $profile"

  tmux new-session -d -s "$profile" \
    "cd $BASE_DIR && source $VENV_PATH/bin/activate && $PYTHON_BIN $ENTRY_SCRIPT --config-py $CONFIG_PY --profile $profile"

  sleep 0.5
  if session_exists "$profile"; then
    echo "[OK] 已启动 $profile"
  else
    echo "[ERROR] 启动失败: $profile"
    echo "最近输出："
    tmux capture-pane -pt "$profile" -S -50 2>/dev/null || true
  fi
}

stop_one() {
  local profile="$1"

  if session_exists "$profile"; then
    echo "[INFO] 停止 $profile"
    tmux kill-session -t "$profile"
    echo "[OK] 已停止 $profile"
  else
    echo "[INFO] $profile 未运行"
  fi
}

restart_one() {
  local profile="$1"
  stop_one "$profile" || true
  sleep 1
  start_one "$profile"
}

attach_one() {
  local profile="$1"

  if session_exists "$profile"; then
    tmux attach -t "$profile"
  else
    echo "[ERROR] $profile 不存在"
  fi
}

logs_one() {
  local profile="$1"

  if session_exists "$profile"; then
    tmux capture-pane -pt "$profile" -S -200 | less
  else
    echo "[ERROR] $profile 不存在"
  fi
}

status_all() {
  echo "==== tmux sessions ===="
  tmux ls 2>/dev/null || echo "无运行实例"

  echo
  echo "==== profile 状态 ===="
  for p in $(list_profiles); do
    if session_exists "$p"; then
      echo "[RUNNING] $p"
    else
      echo "[STOPPED] $p"
    fi
  done
}

start_all() {
  for p in $(list_profiles); do
    start_one "$p"
  done
}

stop_all() {
  for p in $(list_profiles); do
    stop_one "$p"
  done
}

# ========= 主逻辑 =========

ACTION="${1:-}"

case "$ACTION" in
  status)
    status_all
    ;;
  list)
    list_profiles
    ;;
  start)
    TARGET="${2:-}"
    [[ -z "$TARGET" ]] && usage && exit 1
    [[ "$TARGET" == "all" ]] && start_all || start_one "$TARGET"
    ;;
  stop)
    TARGET="${2:-}"
    [[ -z "$TARGET" ]] && usage && exit 1
    [[ "$TARGET" == "all" ]] && stop_all || stop_one "$TARGET"
    ;;
  restart)
    TARGET="${2:-}"
    [[ -z "$TARGET" ]] && usage && exit 1
    [[ "$TARGET" == "all" ]] && start_all || restart_one "$TARGET"
    ;;
  attach)
    attach_one "${2:-}"
    ;;
  logs)
    logs_one "${2:-}"
    ;;
  *)
    usage
    ;;
esac
