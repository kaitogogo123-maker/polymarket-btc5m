#!/bin/zsh
# Start collector (if not running) and dashboard, then open the page.
cd "$(dirname "$0")"
AGENT=gui/$(id -u)/com.kaito.polymarket.dashboard
if launchctl print $AGENT >/dev/null 2>&1; then   # installed by deploy/autostart.sh: launchd owns the processes
  launchctl kickstart -k $AGENT
  PID=$$
else
  PY=.venv/bin/python
  setopt NO_BG_NICE      # zsh would otherwise lower the priority of background jobs
  pgrep -f "python[0-9.]* ([^ ]*/)?src/collect_live\.py" >/dev/null || {
    nohup $PY src/collect_live.py >> out/collect_live.log 2>&1 &
  }
  [ -f .push_token ] && { pgrep -f "python[0-9.]* ([^ ]*/)?src/push_state\.py" >/dev/null || {
    nohup $PY src/push_state.py >> out/push_state.log 2>&1 &
  }; }
  pkill -f "python[0-9.]* ([^ ]*/)?src/dashboard\.py"
  nohup $PY src/dashboard.py > out/dashboard.log 2>&1 &
  PID=$!
fi
for i in {1..60}; do
  curl -s -o /dev/null http://127.0.0.1:8791/ && { open http://127.0.0.1:8791; exit 0; }
  kill -0 $PID 2>/dev/null || break
  sleep 1
done
echo "页面服务没有起来，日志最后几行：" >&2
tail -5 out/dashboard.log >&2
exit 1
