@echo off
cd /d E:\codes\webapp
"C:\Program Files\Git\cmd\git.exe" add -A
"C:\Program Files\Git\cmd\git.exe" commit -m "update"
"C:\Program Files\Git\cmd\git.exe" push
echo.
echo === Code pushed. Now updating server... ===
ssh root@168.144.109.137 "cd /root/eggy-aitest && cp -r static/screenshots /tmp/screenshots_bak 2>/dev/null; git pull && cp -r /tmp/screenshots_bak/* static/screenshots/ 2>/dev/null; rm -rf /tmp/screenshots_bak; systemctl restart webapp"
echo.
echo === Done! Website updated. ===
pause
