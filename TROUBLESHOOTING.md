# QBot Troubleshooting Guide

## Common Issues and Fixes

### 1. SSL Certificate Errors

**Error:** `SSL certificate problem: unable to get local issuer certificate` or HTTP 526

**Causes:**

- Cloudflare proxy (orange cloud) enabled
- SSL certificate not obtained
- Nginx missing SSL block

**Solutions:**

```bash
# Check if SSL cert exists
ls -la /etc/letsencrypt/live/

# Regenerate SSL
certbot --nginx -d $DOMAIN --reinstall

# Verify nginx config has SSL block
cat /etc/nginx/sites-enabled/qbot | grep "listen 443"

# Manual SSL config if certbot didn't add it
cat > /etc/nginx/sites-available/qbot << EOF
server {
    listen 80;
    server_name $DOMAIN;
    return 301 https://\$host\$request_uri;
}

server {
    listen 443 ssl;
    server_name $DOMAIN;

    ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:$BOT_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF

nginx -t && systemctl reload nginx
```

**Cloudflare Users:** Use **gray cloud (DNS only)** mode for Let's Encrypt to work.

---

### 2. Port Already in Use

**Error:** Bot container unhealthy or port conflict

**Check:**

```bash
# See what's using your configured port
docker ps
netstat -tlnp | grep $BOT_PORT

# Check bot logs
cd /opt/qbot && docker compose logs bot --tail 20
```

**Solution:** Set `BOT_PORT` GitHub secret to an unused port.

---

### 3. Bot Not Responding to Messages

**Check webhook status:**

```bash
curl https://api.telegram.org/bot<TOKEN>/getWebhookInfo
```

**Check nginx is proxying correctly:**

```bash
# Test bot directly (use your BOT_PORT)
curl http://localhost:$BOT_PORT/health

# Test via HTTPS
curl -k https://$DOMAIN/health
```

**Check bot logs:**

```bash
cd /opt/qbot && docker compose logs bot -f
```

---

### 4. Build Fails - Missing package-lock.json

**Error:** `npm ci` requires `package-lock.json`

**Solution:**

```bash
# Generate locally
npm install
git add package-lock.json
git commit -m "Add package-lock.json"
git push
```

---

### 5. sudo: command not found

If your VPS runs as root, `sudo` isn't installed. The GitHub Actions workflow handles this automatically.

---

### 6. Database Migration Fails

**Check:**

```bash
cd /opt/qbot
docker compose exec bot npm run db:push
```

**Full reset:**

```bash
docker compose down -v  # Warning: deletes data
docker compose up -d
docker compose exec bot npm run db:push
```

---

## Useful Debug Commands

```bash
# Check your configured port
cat /opt/qbot/.env | grep BOT_PORT

# Container status
docker compose ps

# Bot logs
docker compose logs bot -f

# Rebuild everything
docker compose down
docker compose build --no-cache
docker compose up -d

# Check nginx config
nginx -t
cat /etc/nginx/sites-enabled/qbot

# Check SSL certificate
certbot certificates

# Test health endpoint (use your BOT_PORT)
curl http://localhost:$BOT_PORT/health

# Check webhook
curl https://api.telegram.org/bot<TOKEN>/getWebhookInfo
```
