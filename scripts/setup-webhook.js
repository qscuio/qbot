import { config } from '../src/config.js';
import { registerWebhook, registerCommands } from '../src/telegram/api.js';

const commands = [
  { command: 'start', description: 'Show help message' },
  { command: 'help', description: 'Show help message' },
  { command: 'new', description: 'Start new chat' },
  { command: 'chats', description: 'List/switch chats' },
  { command: 'rename', description: 'Rename current chat' },
  { command: 'clear', description: 'Clear current chat' },
  { command: 'export', description: 'Export chat to notes' },
  { command: 'ai', description: 'Ask AI a question' },
  { command: 'providers', description: 'Select AI provider' },
  { command: 'models', description: 'Select AI model' },
  { command: 'users', description: 'List allowed users (owner)' },
  { command: 'adduser', description: 'Add user (owner)' },
  { command: 'deluser', description: 'Remove user (owner)' },
];

async function setup() {
  console.log('🔧 Setting up webhook and commands...\n');
  
  if (!config.webhookUrl) {
    console.error('❌ WEBHOOK_URL is not set. Please set it in your .env file.');
    process.exit(1);
  }
  
  // Register webhook
  console.log(`📡 Registering webhook: ${config.webhookUrl}/webhook`);
  const webhookResult = await registerWebhook(config.webhookUrl, config.botSecret);
  console.log('Webhook result:', webhookResult);
  
  // Register commands
  console.log('\n📋 Registering commands...');
  const commandsResult = await registerCommands(commands);
  console.log('Commands result:', commandsResult);
  
  console.log('\n✅ Setup complete!');
}

setup().catch(console.error);
