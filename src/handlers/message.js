import * as telegram from '../telegram/api.js';
import {
  handleStart,
  handleNewChat,
  handleChats,
  handleRename,
  handleClear,
  handleAI,
  handleForwardedMessage,
  handleProviders,
  handleModels,
  handleExport,
  handleUsers,
  handleAddUser,
  handleDelUser,
  processAIRequest,
} from './commands/index.js';

// Re-export processAIRequest for callback.js
export { processAIRequest };

// Random reactions list
const REACTIONS = ['👍', '❤️', '🔥', '🎉', '🤔', '👀', '💯', '🚀'];

// Command handlers registry
const commands = {
  '/start': handleStart,
  '/help': handleStart,
  '/ai': handleAI,
  '/providers': handleProviders,
  '/models': handleModels,
  '/new': handleNewChat,
  '/chats': handleChats,
  '/rename': handleRename,
  '/clear': handleClear,
  '/export': handleExport,
  '/adduser': handleAddUser,
  '/deluser': handleDelUser,
  '/users': handleUsers,
};

// Main message handler
export async function handleMessage(message) {
  const chatId = message.chat.id;
  const text = message.text || '';
  
  // Check for commands
  for (const [cmd, handler] of Object.entries(commands)) {
    if (text.startsWith(cmd)) {
      return handler(message);
    }
  }
  
  // Check if it's a forwarded message - analyze it
  if (message.forward_from || message.forward_from_chat || message.forward_origin) {
    return handleForwardedMessage(message);
  }
  
  // Default: random reaction
  const emoji = REACTIONS[Math.floor(Math.random() * REACTIONS.length)];
  return telegram.setMessageReaction(chatId, message.message_id, emoji, emoji === '🎉');
}
