#!/bin/bash
# One-click deploy script for search eval webapp
# Run on the DO server after SSH login

set -e

echo "=== Installing dependencies ==="
apt update -y
apt install -y python3 python3-pip python3-venv git

echo "=== Cloning repo ==="
cd /root
rm -rf eggy-aitest
git clone https://github.com/doublejjjjj/eggy-aitest.git
cd eggy-aitest

echo "=== Setting up Python venv ==="
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn python-multipart httpx aiosqlite openpyxl

echo "=== Setting up environment ==="
export KIMI_API_KEY="sk-EuquuBvCORXKvLvimzUs7hDgVhZgzy74Dn9E04iNjTvvLBMP"
export PORT=80

echo "=== Creating systemd service ==="
cat > /etc/systemd/system/webapp.service << 'EOF'
[Unit]
Description=Search Eval Webapp
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/eggy-aitest
Environment=PORT=80
Environment=KIMI_API_KEY=sk-EuquuBvCORXKvLvimzUs7hDgVhZgzy74Dn9E04iNjTvvLBMP
ExecStart=/root/eggy-aitest/venv/bin/python main.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable webapp
systemctl start webapp

echo ""
echo "=== DONE! ==="
echo "Website: http://168.144.109.137"
echo "Service status: systemctl status webapp"
