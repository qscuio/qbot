import * as telegram from '../../telegram/api.js';

// /start and /help command
export async function handleStart(message) {
  const chatId = message.chat.id;
  const helpText = `<b>🤖 AI Chat Bot</b>

Select a command below or just type a message to chat with AI!`;

  const buttons = [
    [{ text: '✨ New Chat', callback_data: 'cmd_new' }, { text: '📂 Chats', callback_data: 'cmd_chats' }],
    [{ text: '🔌 Providers', callback_data: 'cmd_providers' }, { text: '📋 Models', callback_data: 'cmd_models' }],
    [{ text: '📝 Export', callback_data: 'cmd_export' }, { text: '👥 Users', callback_data: 'cmd_users' }],
    [{ text: '🗑️ Clear', callback_data: 'cmd_clear' }],
  ];

  return telegram.sendInlineButtons(chatId, helpText, buttons);
}
