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

```
┌─────────────────┐                      ┌─────────────────┐
│    Client       │  ──── connect ────→  │     Server      │
│  holds PRIVATE  │  ← challenge ──────  │  holds PUBLIC   │
│      KEY        │  ── sign challenge → │      KEY        │
│                 │  ← ✅ success ─────  │                 │
└─────────────────┘                      └─────────────────┘
```

- **GitHub Actions** = Client → needs **Private Key** (in GitHub Secrets)
- **Your VPS** = Server → needs **Public Key** (in `~/.ssh/authorized_keys`)

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
```

### Step 2: Set Up DNS Record

Point a domain/subdomain to your VPS IP address:

| Type | Name | Value                              |
| ---- | ---- | ---------------------------------- |
| A    | bot  | Your VPS IP (e.g., `123.45.67.89`) |

Your `WEBHOOK_URL` will be `https://bot.yourdomain.com`

> The GitHub Actions workflow will **automatically configure Nginx and SSL** on first deployment.

### Step 3: Fork or Clone

```bash
git clone git@github.com:your-username/qbot.git
cd qbot
```

### Step 4: Create a Telegram Bot

1. Open [@BotFather](https://t.me/botfather) in Telegram
2. Send `/newbot` and follow the prompts
3. Copy the Bot Token

### Step 5: Get API Keys

Get at least one API key:

- [Google AI Studio](https://aistudio.google.com/) - Gemini
- [OpenAI Platform](https://platform.openai.com/api-keys) - OpenAI
- [Anthropic Console](https://console.anthropic.com/) - Claude

### Step 6: Configure GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions.

**Required Secrets:**

| Secret           | Description                           |
| ---------------- | ------------------------------------- |
| `VPS_HOST`       | Your VPS IP address or hostname       |
| `VPS_USER`       | SSH username (e.g., `root`)           |
| `VPS_SSH_KEY`    | Private SSH key (from Step 1)         |
| `BOT_TOKEN`      | Telegram bot token from BotFather     |
| `BOT_SECRET`     | Random string for webhook security    |
| `WEBHOOK_URL`    | `https://bot.yourdomain.com` (Step 2) |
| `GEMINI_API_KEY` | Gemini API key                        |

**Optional Secrets:**

| Secret           | Description                       |
| ---------------- | --------------------------------- |
| `BOT_PORT`       | Port to run bot (default: `3000`) |
| `OPENAI_API_KEY` | OpenAI API key                    |
| `CLAUDE_API_KEY` | Claude API key                    |
| `ALLOWED_USERS`  | Comma-separated Telegram user IDs |

### Step 7: Deploy

Push to `main` branch or go to Actions → Deploy to VPS → Run workflow.

The workflow automatically:

- ✅ Installs Docker (if needed)
- ✅ Installs Nginx and obtains SSL certificate
- ✅ Deploys the bot with Docker Compose
- ✅ Registers the Telegram webhook

### Step 8: Test

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
# Clone and setup
git clone git@github.com:your-username/qbot.git
cd qbot
cp .env.example .env  # Edit with your values

# Start with Docker Compose
docker compose up -d

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
