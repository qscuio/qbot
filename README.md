# QBot - Multi-Provider AI Telegram Bot

A feature-rich Telegram Bot with multi-provider AI support (Gemini, OpenAI, Claude), deployed via Docker to your VPS.

## Features

- 🤖 **Multi-Provider AI** - Switch between Gemini, OpenAI, and Claude
- 🔄 **Model Selection** - Choose models per provider
- 💭 **Thinking Process** - See AI reasoning (where supported)
- 💾 **Persistent Settings** - Provider and model preferences saved per user
- 🔒 **User Whitelist** - Restrict bot access to specific users
- ⚡ **Inline Buttons** - Interactive provider/model selection

## Quick Deploy (GitHub Actions)

### Step 1: Set Up SSH Key (on your VPS)

#### Why Private Key?

SSH authentication flow:

```
┌─────────────────┐                      ┌─────────────────┐
│    Client       │                      │     Server      │
│ (initiates SSH) │  ──── connect ────→  │  (accepts SSH)  │
│                 │                      │                 │
│  holds PRIVATE  │  ← challenge ──────  │  holds PUBLIC   │
│      KEY        │                      │      KEY        │
│                 │  ── sign challenge → │                 │
│                 │                      │  verify with    │
│                 │  ← ✅ success ─────  │  public key     │
└─────────────────┘                      └─────────────────┘
```

In our case:

- **GitHub Actions** = Client → needs **Private Key** (stored in GitHub Secrets)
- **Your VPS** = Server → needs **Public Key** (stored in `~/.ssh/authorized_keys`)

#### Generate Key on VPS

```bash
# SSH into your VPS
ssh your-user@your-vps

# Generate key pair
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/github_actions

# Add public key to authorized_keys
cat ~/.ssh/github_actions.pub >> ~/.ssh/authorized_keys

# Display private key (copy this to GitHub Secret VPS_SSH_KEY)
cat ~/.ssh/github_actions
# Copy the entire output (including -----BEGIN/END lines)
```

### Step 2: Fork or Clone

```bash
git clone git@github.com:your-username/qbot.git
cd qbot
```

### Step 3: Create a Telegram Bot

1. Open [@BotFather](https://t.me/botfather) in Telegram
2. Send `/newbot` and follow the prompts
3. Copy the Bot Token

### Step 4: Get API Keys

Get at least one API key:

- [Google AI Studio](https://aistudio.google.com/) - Gemini
- [OpenAI Platform](https://platform.openai.com/api-keys) - OpenAI
- [Anthropic Console](https://console.anthropic.com/) - Claude

### Step 5: Set Up WEBHOOK_URL

Telegram requires HTTPS. Choose one option:

**Option A: Own Domain + Let's Encrypt (Recommended)**

```bash
# On your VPS, install Caddy (auto HTTPS)
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install caddy

# Create Caddyfile
echo "bot.yourdomain.com {
    reverse_proxy localhost:3000
}" | sudo tee /etc/caddy/Caddyfile

sudo systemctl restart caddy
```

Your `WEBHOOK_URL` = `https://bot.yourdomain.com`

**Option B: Cloudflare Tunnel (Free, No Domain Needed)**

```bash
# Install cloudflared
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
chmod +x /usr/local/bin/cloudflared

# Create a named tunnel (requires free Cloudflare account)
cloudflared tunnel login
cloudflared tunnel create qbot
cloudflared tunnel route dns qbot bot.yourdomain.com
```

> ⚠️ **Note**: Quick tunnels (`cloudflared tunnel --url`) are for testing only - they have rate limits and change URLs on restart.

### Step 6: Configure GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions.

**Required Secrets:**

| Secret           | Description                             |
| ---------------- | --------------------------------------- |
| `VPS_HOST`       | Your VPS IP address or hostname         |
| `VPS_USER`       | SSH username (e.g., `root` or `deploy`) |
| `VPS_SSH_KEY`    | Private SSH key (from Step 1)           |
| `BOT_TOKEN`      | Telegram bot token from BotFather       |
| `BOT_SECRET`     | Random string for webhook security      |
| `WEBHOOK_URL`    | Public URL (from Step 5)                |
| `GEMINI_API_KEY` | Gemini API key                          |

**Optional Secrets:**

| Secret           | Description                       |
| ---------------- | --------------------------------- |
| `OPENAI_API_KEY` | OpenAI API key                    |
| `CLAUDE_API_KEY` | Claude API key                    |
| `ALLOWED_USERS`  | Comma-separated Telegram user IDs |

### Step 7: Prepare Your VPS

The GitHub Actions workflow will **automatically install Docker** if it's not present. Just ensure:

- SSH access is configured (Step 1)
- Your user has `sudo` privileges

### Step 8: Deploy

Push to `main` branch or go to Actions → Deploy to VPS → Run workflow.

### Step 9: Test

Open your bot in Telegram and send `/start`!

## Commands

| Command      | Description        |
| ------------ | ------------------ |
| `/start`     | Show help message  |
| `/ai <text>` | Ask AI a question  |
| `/providers` | Select AI provider |
| `/models`    | Select AI model    |

## Local Development

```bash
# Clone the repository
git clone git@github.com:your-username/qbot.git
cd qbot

# Copy environment file
cp .env.example .env
# Edit .env with your values

# Start with Docker Compose
docker compose up -d

# View logs
docker compose logs -f bot

# Run database migrations
docker compose exec bot npm run db:push

# Setup webhook (use ngrok for local testing)
docker compose exec bot npm run setup-webhook
```

## Architecture

```
src/
├── index.js           # Express server entry point
├── config.js          # Environment configuration
├── telegram/          # Telegram API wrappers
├── handlers/          # Message/callback handlers
├── providers/         # AI providers (Gemini, OpenAI, Claude)
├── cache/             # Redis cache adapter
└── db/                # Prisma ORM for PostgreSQL
```

### Adding New Features

**Add a new command:**

1. Edit `src/handlers/message.js`
2. Add handler function and register in `commands` object

**Add a new AI provider:**

1. Create `src/providers/newprovider.js`
2. Add to registry in `src/providers/index.js`
3. Add case in `src/handlers/message.js` → `processAIRequest()`

## License

MIT
