#!/bin/zsh
# Keep the three processes alive with launchd: start at login, restart on crash.
#   zsh deploy/autostart.sh install | remove
ROOT=${0:A:h:h}
LA=~/Library/LaunchAgents
DOMAIN=gui/$(id -u)
NAMES=(collect_live dashboard push_state)

plist() {
  cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>com.kaito.polymarket.$1</string>
<key>ProgramArguments</key><array><string>$ROOT/.venv/bin/python</string><string>$ROOT/src/$1.py</string></array>
<key>WorkingDirectory</key><string>$ROOT</string>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>ThrottleInterval</key><integer>10</integer>
<key>ProcessType</key><string>Interactive</string>
<key>StandardOutPath</key><string>$ROOT/out/$1.log</string>
<key>StandardErrorPath</key><string>$ROOT/out/$1.log</string>
</dict></plist>
XML
}

remove() {
  for n in $NAMES; do
    launchctl bootout $DOMAIN/com.kaito.polymarket.$n 2>/dev/null
    rm -f $LA/com.kaito.polymarket.$n.plist
  done
}

case $1 in
  install)
    remove
    for n in $NAMES; do pkill -f "python[0-9.]* ([^ ]*/)?src/$n\.py"; done
    # The collector holds its lock until it has exited; a successor started earlier refuses to run.
    for i in {1..30}; do
      pgrep -f "python[0-9.]* ([^ ]*/)?src/(collect_live|dashboard|push_state)\.py" >/dev/null || break
      sleep 1
    done
    mkdir -p $LA
    for n in $NAMES; do
      plist $n > $LA/com.kaito.polymarket.$n.plist
      launchctl bootstrap $DOMAIN $LA/com.kaito.polymarket.$n.plist || { echo "失败: $n" >&2; exit 1; }
    done
    sleep 3
    launchctl list | grep com.kaito.polymarket
    echo "已安装。卸载: zsh deploy/autostart.sh remove"
    ;;
  remove)
    remove
    echo "已卸载，三个进程已停。手动启动: ./run.sh"
    ;;
  *)
    echo "用法: zsh deploy/autostart.sh install|remove" >&2; exit 2
    ;;
esac
