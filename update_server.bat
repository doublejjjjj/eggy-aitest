@echo off
cd /d E:\codes\webapp
"C:\Program Files\Git\cmd\git.exe" add -A
"C:\Program Files\Git\cmd\git.exe" commit -m "update"
"C:\Program Files\Git\cmd\git.exe" push
echo.
echo === Code pushed. Now updating server... ===
ssh root@168.144.109.137 "cd /root/eggy-aitest && git pull && systemctl restart webapp"
echo.
echo === Done! Website updated. ===
pause
