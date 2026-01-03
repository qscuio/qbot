import { config } from '../config.js';

const API_BASE = `https://api.telegram.org/bot${config.botToken}`;

// Generic API call helper
async function callApi(method, params = {}) {
  const url = `${API_BASE}/${method}`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  return response.json();
}

// Send plain text message
export async function sendMessage(chatId, text) {
  return callApi('sendMessage', { chat_id: chatId, text });
}

// Send HTML formatted message
export async function sendHtmlMessage(chatId, text) {
  const result = await callApi('sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'HTML',
  });
  if (!result.ok) {
    console.error('sendHtmlMessage failed:', result.description, 'Text length:', text.length);
  }
  return result;
}

// Send message with inline keyboard
export async function sendInlineButtons(chatId, text, buttons, parseMode = 'HTML') {
  return callApi('sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: parseMode,
    reply_markup: { inline_keyboard: buttons },
  });
}

// Answer callback query (acknowledge button press)
export async function answerCallbackQuery(callbackQueryId, text = null) {
  const params = { callback_query_id: callbackQueryId };
  if (text) params.text = text;
  return callApi('answerCallbackQuery', params);
}

// Set message reaction
export async function setMessageReaction(chatId, messageId, emoji, isBig = false) {
  return callApi('setMessageReaction', {
    chat_id: chatId,
    message_id: messageId,
    reaction: [{ type: 'emoji', emoji }],
    is_big: isBig,
  });
}

// Answer inline query
export async function answerInlineQuery(inlineQueryId, results) {
  return callApi('answerInlineQuery', {
    inline_query_id: inlineQueryId,
    results,
  });
}

// Send chat action (typing, etc.)
export async function sendChatAction(chatId, action = 'typing') {
  return callApi('sendChatAction', {
    chat_id: chatId,
    action,
  });
}

// Edit an existing message
export async function editMessageText(chatId, messageId, text, parseMode = 'HTML') {
  return callApi('editMessageText', {
    chat_id: chatId,
    message_id: messageId,
    text,
    parse_mode: parseMode,
  });
}

// Send long text (split into chunks if needed, at newline boundaries)
export async function sendLongHtmlMessage(chatId, text) {
  const MAX_LENGTH = 4000; // Leave some room for safety
  
  if (text.length <= MAX_LENGTH) {
    return sendHtmlMessage(chatId, text);
  }
  
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= MAX_LENGTH) {
      await sendHtmlMessage(chatId, remaining);
      break;
    }
    
    // Find a good place to split (newline, then space)
    let splitAt = remaining.lastIndexOf('\n', MAX_LENGTH);
    if (splitAt < MAX_LENGTH * 0.5) {
      splitAt = remaining.lastIndexOf(' ', MAX_LENGTH);
    }
    if (splitAt < MAX_LENGTH * 0.5) {
      splitAt = MAX_LENGTH;
    }
    
    await sendHtmlMessage(chatId, remaining.substring(0, splitAt));
    remaining = remaining.substring(splitAt).trimStart();
    
    // Add small delay to ensure order and avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}

// Register webhook
export async function registerWebhook(url, secret) {
  return callApi('setWebhook', {
    url: `${url}/webhook`,
    secret_token: secret,
  });
}

// Unregister webhook
export async function unregisterWebhook() {
  return callApi('setWebhook', { url: '' });
}

// Register bot commands
export async function registerCommands(commands) {
  return callApi('setMyCommands', { commands });
}

// Escape HTML special characters
export function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Convert markdown to Telegram HTML
export function markdownToHtml(str) {
  return escapeHtml(str)
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/__(.+?)__/g, '<b>$1</b>')
    .replace(/\*(.+?)\*/g, '<i>$1</i>')
    .replace(/_(.+?)_/g, '<i>$1</i>')
    .replace(/```[\w]*\n?([\s\S]+?)```/g, '<pre>$1</pre>')
    .replace(/`(.+?)`/g, '<code>$1</code>')
    .replace(/^#{1,6}\s+(.+)$/gm, '<b>$1</b>')
    .replace(/^\s*[\*\-]\s+(.+)$/gm, '• $1');
}
