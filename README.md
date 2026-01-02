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

### Step 1: Fork or Clone

```bash
git clone git@github.com:your-username/qbot.git
cd qbot
```

### Step 2: Create a Telegram Bot

1. Open [@BotFather](https://t.me/botfather) in Telegram
2. Send `/newbot` and follow the prompts
3. Copy the Bot Token

### Step 3: Get API Keys

Get at least one API key:

- [Google AI Studio](https://aistudio.google.com/) - Gemini
- [OpenAI Platform](https://platform.openai.com/api-keys) - OpenAI
- [Anthropic Console](https://console.anthropic.com/) - Claude

### Step 4: Configure GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions.

**Required Secrets:**

| Secret           | Description                                     |
| ---------------- | ----------------------------------------------- |
| `VPS_HOST`       | Your VPS IP address or hostname                 |
| `VPS_USER`       | SSH username (e.g., `root` or `deploy`)         |
| `VPS_SSH_KEY`    | Private SSH key (Ed25519 recommended)           |
| `BOT_TOKEN`      | Telegram bot token from BotFather               |
| `BOT_SECRET`     | Random string for webhook security              |
| `WEBHOOK_URL`    | Public URL (e.g., `https://bot.yourdomain.com`) |
| `GEMINI_API_KEY` | Gemini API key                                  |

**Optional Secrets:**

| Secret           | Description                       |
| ---------------- | --------------------------------- |
| `OPENAI_API_KEY` | OpenAI API key                    |
| `CLAUDE_API_KEY` | Claude API key                    |
| `ALLOWED_USERS`  | Comma-separated Telegram user IDs |

### Step 5: Prepare Your VPS

The GitHub Actions workflow will **automatically install Docker** if it's not present. Just ensure:

- SSH access is configured
- Your user has `sudo` privileges

### Step 6: Deploy

Push to `main` branch or go to Actions → Deploy to VPS → Run workflow.

### Step 7: Test

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
