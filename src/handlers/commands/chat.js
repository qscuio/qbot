import * as telegram from '../../telegram/api.js';
import { 
  getUserSettings,
  createChat,
  getActiveChat,
  getUserChats,
  renameChat,
  clearChatMessages,
  updateChatSummary,
} from '../../db/index.js';

// /new - Create new chat
export async function handleNewChat(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  await createChat(userId);
  return telegram.sendHtmlMessage(chatId, `✨ <b>New chat created!</b>\n\nSend me a message to start chatting.`);
}

// /chats - List user's chats
export async function handleChats(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const settings = await getUserSettings(userId);
  const chats = await getUserChats(userId, 10);
  
  if (chats.length === 0) {
    return telegram.sendMessage(chatId, 'No chats yet. Send a message to start!');
  }
  
  const buttons = chats.map(chat => [{
    text: `${chat.id === settings.activeChatId ? '✅ ' : ''}${chat.title.substring(0, 30)}`,
    callback_data: `switch_chat_${chat.id}`,
  }]);
  
  const text = `<b>📂 Your Chats:</b>\n\n<i>Tap to switch</i>`;
  return telegram.sendInlineButtons(chatId, text, buttons);
}

// /rename <title> - Rename current chat
export async function handleRename(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const newTitle = (message.text || '').replace('/rename', '').trim();
  
  if (!newTitle) {
    return telegram.sendMessage(chatId, 'Usage: /rename <new title>');
  }
  
  const activeChat = await getActiveChat(userId);
  await renameChat(activeChat.id, newTitle);
  
  return telegram.sendMessage(chatId, `✅ Chat renamed to: <b>${telegram.escapeHtml(newTitle)}</b>`);
}

// /clear - Clear current chat
export async function handleClear(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const activeChat = await getActiveChat(userId);
  await clearChatMessages(activeChat.id);
  await updateChatSummary(activeChat.id, null);
  
  return telegram.sendMessage(chatId, '🗑️ Chat history cleared!');
}
