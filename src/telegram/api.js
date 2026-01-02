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
  return callApi('sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'HTML',
  });
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

// Send long text (split into chunks if needed)
export async function sendLongHtmlMessage(chatId, text) {
  const MAX_LENGTH = 4096;
  for (let i = 0; i < text.length; i += MAX_LENGTH) {
    await sendHtmlMessage(chatId, text.substring(i, i + MAX_LENGTH));
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
