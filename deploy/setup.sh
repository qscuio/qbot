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

# Optional: auto-configure Nginx reverse proxy when WEBHOOK_URL is set.
read_env_value() {
    local key="$1"
    grep -E "^${key}=" /opt/qbot/.env | tail -n1 | cut -d= -f2- | tr -d '\r'
}

WEBHOOK_URL="${WEBHOOK_URL:-$(read_env_value WEBHOOK_URL)}"
API_PORT="${API_PORT:-$(read_env_value API_PORT)}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-$(read_env_value LETSENCRYPT_EMAIL)}"

configure_nginx_proxy() {
    local webhook_url="${WEBHOOK_URL:-}"
    local api_port="${API_PORT:-8080}"
    local letsencrypt_email="${LETSENCRYPT_EMAIL:-}"

    if [ -z "$webhook_url" ]; then
        echo "WEBHOOK_URL is empty; skipping Nginx setup"
        return 0
    fi

    webhook_url="${webhook_url%/}"
    if [[ ! "$webhook_url" =~ ^https?:// ]]; then
        echo "Invalid WEBHOOK_URL: $webhook_url"
        return 0
    fi

    local scheme="${webhook_url%%://*}"
    local rest="${webhook_url#*://}"
    local hostport="${rest%%/*}"
    local host="${hostport%%:*}"
    local cert_email="${letsencrypt_email:-admin@${host}}"
    local cert_path="/etc/letsencrypt/live/${host}/fullchain.pem"
    local key_path="/etc/letsencrypt/live/${host}/privkey.pem"

    if ! command -v nginx &>/dev/null; then
        sudo apt-get update
        sudo apt-get install -y nginx
    fi

    if [ "$scheme" = "https" ] && { [ ! -f "$cert_path" ] || [ ! -f "$key_path" ]; }; then
        if ! command -v certbot &>/dev/null; then
            sudo apt-get update
            sudo apt-get install -y certbot python3-certbot-nginx
        fi
        sudo systemctl enable nginx || true
        sudo systemctl start nginx || true
        if ! sudo certbot certonly --nginx \
            -d "$host" \
            --non-interactive \
            --agree-tos \
            --email "$cert_email"; then
            echo "ERROR: certbot failed for ${host}"
            echo "Hint: for Cloudflare, use gray cloud (DNS only) during issuance."
            exit 1
        fi
    fi

    if [ "$scheme" = "https" ]; then
        if [ ! -f "$cert_path" ] || [ ! -f "$key_path" ]; then
            echo "ERROR: HTTPS requested but certificate files are missing:"
            echo "  $cert_path"
            echo "  $key_path"
            exit 1
        fi
        cat <<EOF | sudo tee /etc/nginx/sites-available/qbot.conf >/dev/null
server {
    listen 80;
    server_name ${host};
    location /.well-known/acme-challenge/ { root /var/www/html; }
    location / { return 301 https://\$host\$request_uri; }
}
server {
    listen 443 ssl http2;
    server_name ${host};
    ssl_certificate ${cert_path};
    ssl_certificate_key ${key_path};
    client_max_body_size 2m;
    location /telegram/webhook {
        proxy_pass http://127.0.0.1:${api_port}/telegram/webhook;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
    location / {
        proxy_pass http://127.0.0.1:${api_port};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF
    else
        cat <<EOF | sudo tee /etc/nginx/sites-available/qbot.conf >/dev/null
server {
    listen 80;
    server_name ${host};
    client_max_body_size 2m;
    location /telegram/webhook {
        proxy_pass http://127.0.0.1:${api_port}/telegram/webhook;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
    location / {
        proxy_pass http://127.0.0.1:${api_port};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF
    fi

    sudo ln -sfn /etc/nginx/sites-available/qbot.conf /etc/nginx/sites-enabled/qbot.conf
    sudo rm -f /etc/nginx/sites-enabled/default
    sudo nginx -t
    sudo systemctl enable nginx
    sudo systemctl restart nginx
    echo "Nginx proxy configured for ${host}"
}

configure_nginx_proxy

# Build and install binary
cargo build --release
if [ -f /etc/systemd/system/qbot.service ] || [ -f /lib/systemd/system/qbot.service ]; then
    if sudo systemctl is-active --quiet qbot; then
        echo "Stopping qbot before binary update..."
        sudo systemctl stop qbot
    fi
fi
cp target/release/qbot /opt/qbot/qbot

# Install systemd service
sed "s/^User=.*/User=$DEPLOY_USER/" deploy/qbot.service | sudo tee /etc/systemd/system/qbot.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable qbot
sudo systemctl start qbot

echo "qbot deployed! Check: sudo journalctl -u qbot -f"
