# QBot - Multi-Provider AI Telegram Bot

A feature-rich Telegram Bot with multi-provider AI support (Groq, Gemini, OpenAI, Claude), chat history, and knowledge export.

## Features

- 🤖 **Multi-Provider AI** - Groq (default), Gemini, OpenAI, Claude
- 💬 **Chat History** - Multiple conversations with context
- 📝 **Knowledge Export** - Export chats to markdown, push to git
- 🔄 **Model Selection** - Choose models per provider
- 💭 **Thinking Process** - See AI reasoning (where supported)
- 💾 **Persistent Settings** - Provider, model, chats saved per user
- 🔒 **User Whitelist** - Restrict bot access to specific users

## Commands

| Command           | Description                |
| ----------------- | -------------------------- |
| `/new`            | Start a new chat           |
| `/chats`          | List/switch between chats  |
| `/rename <title>` | Rename current chat        |
| `/clear`          | Clear current chat history |
| `/export`         | Export chat to git repo    |
| `/ai <text>`      | Ask AI a question          |
| `/providers`      | Select AI provider         |
| `/models`         | Select model               |

## Quick Deploy (GitHub Actions)

### Step 1: Set Up SSH Key (on your VPS)

```bash
# SSH into your VPS
ssh your-user@your-vps

# Generate key pair
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/github_actions

# Add public key to authorized_keys (for deployment SSH)
cat ~/.ssh/github_actions.pub >> ~/.ssh/authorized_keys

# Display private key (copy this to GitHub Secret VPS_SSH_KEY)
cat ~/.ssh/github_actions
```

**For `/export` feature** (push chats to notes repo):

1. Show the public key on VPS:
   ```bash
   cat ~/.ssh/github_actions.pub
   ```
2. Go to GitHub → your notes repo (e.g., `qscuio/qnote`) → Settings → Deploy keys
3. Click "Add deploy key", paste the public key, check **"Allow write access"**, save

### Step 2: Set Up DNS Record

Point a domain/subdomain to your VPS IP address:

| Type | Name | Value                              |
| ---- | ---- | ---------------------------------- |
| A    | bot  | Your VPS IP (e.g., `123.45.67.89`) |

Your `WEBHOOK_URL` will be `https://bot.yourdomain.com`

> ⚠️ **Cloudflare Users**: Use **gray cloud (DNS only)** mode for Let's Encrypt to work.

### Step 3: Create a Telegram Bot

1. Open [@BotFather](https://t.me/botfather) in Telegram
2. Send `/newbot` and follow the prompts
3. Copy the Bot Token

### Step 4: Configure GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions.

**VPS & Deployment:**

| Secret        | Description                     |
| ------------- | ------------------------------- |
| `VPS_HOST`    | Your VPS IP address or hostname |
| `VPS_USER`    | SSH username (e.g., `root`)     |
| `VPS_SSH_KEY` | Private SSH key (from Step 1)   |

**Bot Configuration:**

| Secret          | Description                        |
| --------------- | ---------------------------------- |
| `BOT_TOKEN`     | Telegram bot token from BotFather  |
| `BOT_SECRET`    | Random string for webhook security |
| `WEBHOOK_URL`   | `https://bot.yourdomain.com`       |
| `BOT_PORT`      | Port to run bot (default: `3000`)  |
| `ALLOWED_USERS` | Comma-separated Telegram user IDs  |

**AI Provider API Keys:**

| Secret           | Description                     |
| ---------------- | ------------------------------- |
| `GROQ_API_KEY`   | Groq API key (default provider) |
| `GEMINI_API_KEY` | Google Gemini API key           |
| `OPENAI_API_KEY` | OpenAI API key                  |
| `CLAUDE_API_KEY` | Anthropic Claude API key        |

**Features (Optional):**

| Secret       | Description                                        |
| ------------ | -------------------------------------------------- |
| `NOTES_REPO` | Git repo for /export (uses `VPS_SSH_KEY` for auth) |

### Step 5: Deploy

Push to `main` branch or go to Actions → Deploy to VPS → Run workflow.

The workflow automatically:

- ✅ Installs Git and Docker (if needed)
- ✅ Installs Nginx and obtains SSL certificate
- ✅ Deploys the bot with Docker Compose
- ✅ Runs database migrations
- ✅ Registers the Telegram webhook

### Step 6: Test

Open your bot in Telegram and send `/start`!

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
├── providers/         # AI providers (Groq, Gemini, OpenAI, Claude)
├── services/          # Export service
├── cache/             # Redis cache adapter
└── db/                # Prisma ORM for PostgreSQL
```

## License

MIT
