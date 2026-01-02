import { handleMessage } from './message.js';
import { handleCallbackQuery } from './callback.js';
import { handleInlineQuery } from './inline.js';
import { config } from '../config.js';

// Check if user is allowed (access control)
function isUserAllowed(userId) {
  if (config.allowedUsers.length === 0) {
    return true; // No restriction if ALLOWED_USERS is not set
  }
  return config.allowedUsers.includes(String(userId));
}

// Main update handler (routes to appropriate handler)
export async function handleUpdate(update) {
  try {
    if ('message' in update) {
      const userId = update.message.from?.id;
      if (!isUserAllowed(userId)) {
        const { sendMessage } = await import('../telegram/api.js');
        return sendMessage(update.message.chat.id, '🚫 Access denied. You are not authorized to use this bot.');
      }
      return handleMessage(update.message);
    }
    
    if ('callback_query' in update) {
      const userId = update.callback_query.from?.id;
      if (!isUserAllowed(userId)) {
        const { answerCallbackQuery } = await import('../telegram/api.js');
        return answerCallbackQuery(update.callback_query.id, '🚫 Access denied.');
      }
      return handleCallbackQuery(update.callback_query);
    }
    
    if ('inline_query' in update) {
      return handleInlineQuery(update.inline_query);
    }
  } catch (error) {
    console.error('Error handling update:', error);
  }
}
