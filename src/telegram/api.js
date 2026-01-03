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

// Send HTML formatted message with retry
export async function sendHtmlMessage(chatId, text, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const result = await callApi('sendMessage', {
      chat_id: chatId,
      text,
      parse_mode: 'HTML',
    });
    
    if (result.ok) {
      return result;
    }
    
    console.error(`sendHtmlMessage attempt ${attempt + 1} failed:`, result.description);
    
    // If HTML parsing error, try without HTML
    if (result.description?.includes('parse')) {
      console.error('HTML parse error, sending as plain text');
      return sendMessage(chatId, text.replace(/<[^>]*>/g, ''));
    }
    
    // Wait before retry
    if (attempt < retries) {
      await new Promise(r => setTimeout(r, 1000));
    }
  }
  
  return { ok: false, description: 'All retries failed' };
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

// Send long text (split into chunks with retry)
export async function sendLongHtmlMessage(chatId, text) {
  const MAX_LENGTH = 3500;
  const chunks = [];
  
  // Split into chunks at newlines
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= MAX_LENGTH) {
      chunks.push(remaining);
      break;
    }
    
    let splitAt = remaining.lastIndexOf('\n', MAX_LENGTH);
    if (splitAt < MAX_LENGTH * 0.3) {
      splitAt = remaining.lastIndexOf(' ', MAX_LENGTH);
    }
    if (splitAt < MAX_LENGTH * 0.3) {
      splitAt = MAX_LENGTH;
    }
    
    chunks.push(remaining.substring(0, splitAt));
    remaining = remaining.substring(splitAt).trimStart();
  }
  
  console.log(`Sending ${chunks.length} chunks to ${chatId}`);
  
  // Send each chunk with delay
  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    console.log(`Sending chunk ${i + 1}/${chunks.length}, length: ${chunk.length}`);
    
    const result = await sendHtmlMessage(chatId, chunk);
    
    if (!result.ok) {
      console.error(`Chunk ${i + 1} failed:`, result.description);
    }
    
    // Delay between chunks (longer for rate limiting)
    if (i < chunks.length - 1) {
      await new Promise(r => setTimeout(r, 800));
    }
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
