#!/bin/bash
# scripts/local-test.sh -- Full local end-to-end test
# Redis is assumed to already be running (redis-cli ping should return PONG).
# PostgreSQL is started via Docker Compose.
set -e

cd "$(dirname "$0")/.."

# 1. Check Redis
if ! redis-cli ping 2>/dev/null | python3 -c "import sys; s=sys.stdin.read(); exit(0 if 'PONG' in s else 1)"; then
    echo "ERROR: Redis is not running. Start it first."
    exit 1
fi
echo "Redis: OK"

# 2. Start PostgreSQL via Docker Compose
echo "Starting PostgreSQL..."
docker compose -f deploy/docker-compose.yml up -d postgres

echo "Waiting for PostgreSQL to be ready..."
until docker compose -f deploy/docker-compose.yml exec -T postgres \
    pg_isready -U qbot -d qbot -q 2>/dev/null; do
    sleep 1
done
echo "PostgreSQL: OK"

# 3. Check for .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "STOP: .env created from .env.example"
    echo "Fill in at minimum:"
    echo "  TUSHARE_TOKEN=<your token>"
    echo "  TELEGRAM_BOT_TOKEN=<your bot token>"
    echo "  REPORT_CHANNEL=<channel id>"
    echo ""
    echo "Then re-run: ./scripts/local-test.sh"
    exit 1
fi

# 4. Run with --run-now (jobs fire sequentially, then API stays alive)
echo ""
echo "Starting qbot with --run-now..."
echo "Jobs will run in sequence, then API will be live at http://localhost:8080"
echo "Press Ctrl+C to stop."
echo ""
cargo run -- --run-now
