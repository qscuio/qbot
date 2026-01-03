import dotenv from "dotenv";
dotenv.config();

// Required environment variables
const required = ["BOT_TOKEN", "BOT_SECRET"];

for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

export const config = {
  // Server
  port: parseInt(process.env.BOT_PORT || process.env.PORT || "3000"),
  nodeEnv: process.env.NODE_ENV || "development",

  // Telegram
  botToken: process.env.BOT_TOKEN,
  botSecret: process.env.BOT_SECRET,
  webhookUrl: process.env.WEBHOOK_URL,

  // AI Providers
  geminiApiKey: process.env.GEMINI_API_KEY || "",
  openaiApiKey: process.env.OPENAI_API_KEY || "",
  claudeApiKey: process.env.CLAUDE_API_KEY || "",
  groqApiKey: process.env.GROQ_API_KEY || "",
  nvidiaApiKey: process.env.NVIDIA_API_KEY || "",

  // Access Control
  allowedUsers: process.env.ALLOWED_USERS
    ? process.env.ALLOWED_USERS.split(",").map((id) => id.trim())
    : [],
  ownerId: process.env.ALLOWED_USERS?.split(",")[0]?.trim() || null,

  // Notes Export
  notesRepo: process.env.NOTES_REPO || "",

  // Database
  databaseUrl: process.env.DATABASE_URL,
  redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
};
