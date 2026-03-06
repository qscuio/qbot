#!/bin/bash
# deploy/setup.sh -- First-run VPS bootstrap
set -e

echo "qbot VPS Setup"
DEPLOY_USER="$(id -un)"
DEPLOY_GROUP="$(id -gn)"

# Install Rust if not present
if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

# Install Docker if not present
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    sudo systemctl enable docker
    sudo systemctl start docker
fi

# Create deployment directory
sudo mkdir -p /opt/qbot
sudo chown "$DEPLOY_USER:$DEPLOY_GROUP" /opt/qbot

# .env is written by GitHub Actions deploy workflow from secrets.
# For local/manual runs only, copy from example and fill in secrets:
if [ ! -f /opt/qbot/.env ]; then
    echo "No .env found at /opt/qbot/.env"
    echo "  For production: trigger a GitHub Actions deploy to write it from secrets."
    echo "  For local test: cp .env.example /opt/qbot/.env and fill in secrets."
    exit 1
fi

# Start database services
cd /opt/qbot
docker compose -f deploy/docker-compose.yml up -d
echo "Waiting for PostgreSQL..."
sleep 10

# Build and install binary
cargo build --release
cp target/release/qbot /opt/qbot/qbot

# Install systemd service
sed "s/^User=.*/User=$DEPLOY_USER/" deploy/qbot.service | sudo tee /etc/systemd/system/qbot.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable qbot
sudo systemctl start qbot

echo "qbot deployed! Check: sudo journalctl -u qbot -f"
