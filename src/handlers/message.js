import * as telegram from '../telegram/api.js';
import { config } from '../config.js';
import { PROVIDERS, DEFAULT_PROVIDER, fetchModels } from '../providers/index.js';
import { callGemini } from '../providers/gemini.js';
import { callOpenAI } from '../providers/openai.js';
import { callClaude } from '../providers/claude.js';
import { callGroq } from '../providers/groq.js';
import { 
  getUserSettings, 
  setUserProvider, 
  setUserModel,
  createChat,
  getActiveChat,
  getUserChats,
  renameChat,
  clearChatMessages,
  saveMessage,
  getChatMessages,
  getMessageCount,
  updateChatSummary,
  getChat,
} from '../db/index.js';
import { exportChatToGit } from '../services/export.js';

// Random reactions list
const REACTIONS = ['👍', '👎', '❤', '🔥', '🥰', '👏', '😁', '🤔', '🤯', '😱', '🤬', '😢', '🎉', '🤩', '🤮', '💩', '🙏', '👌', '🕊', '🤡', '🥱', '🥴', '😍', '🐳', '❤‍🔥', '🌚', '🌭', '💯', '🤣', '⚡', '🍌', '🏆', '💔', '🤨', '😐', '🍓', '🍾', '💋', '🖕', '😈', '😴', '😭', '🤓', '👻', '👨‍💻', '👀', '🎃', '🙈', '😇', '😨', '🤝', '✍', '🤗', '🫡', '🎅', '🎄', '☃', '💅', '🤪', '🗿', '🆒', '💘', '🙉', '🦄', '😘', '💊', '🙊', '😎', '👾', '🤷‍♂', '🤷', '🤷‍♀', '😡'];

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
  
  // Default: random reaction
  const emoji = REACTIONS[Math.floor(Math.random() * REACTIONS.length)];
  return telegram.setMessageReaction(chatId, message.message_id, emoji, emoji === '🎉');
}

// /start and /help command
async function handleStart(message) {
  const chatId = message.chat.id;
  const helpText = `<b>🤖 AI Chat Bot</b>

Select a command below or just type a message to chat with AI!`;

  const buttons = [
    [{ text: '✨ New Chat', callback_data: 'cmd_new' }, { text: '📂 Chats', callback_data: 'cmd_chats' }],
    [{ text: '🔌 Providers', callback_data: 'cmd_providers' }, { text: '📋 Models', callback_data: 'cmd_models' }],
    [{ text: '📝 Export', callback_data: 'cmd_export' }, { text: '🗑️ Clear', callback_data: 'cmd_clear' }],
  ];

  return telegram.sendInlineButtons(chatId, helpText, buttons);
}

// /new - Create new chat
async function handleNewChat(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const chat = await createChat(userId);
  return telegram.sendMessage(chatId, `✨ <b>New chat created!</b>\n\nSend me a message to start chatting.`);
}

// /chats - List user's chats
async function handleChats(message) {
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
async function handleRename(message) {
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
async function handleClear(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const activeChat = await getActiveChat(userId);
  await clearChatMessages(activeChat.id);
  await updateChatSummary(activeChat.id, null);
  
  return telegram.sendMessage(chatId, '🗑️ Chat history cleared!');
}

// /export - Export chat to git
async function handleExport(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  if (!config.notesRepo) {
    return telegram.sendMessage(chatId, '❌ Notes repo not configured. Set NOTES_REPO environment variable.');
  }
  
  await telegram.sendMessage(chatId, '📝 Exporting chat...');
  
  try {
    const result = await exportChatToGit(userId);
    return telegram.sendMessage(chatId, `✅ Chat exported!\n\n📄 ${result.filename}`);
  } catch (error) {
    return telegram.sendMessage(chatId, `❌ Export failed: ${error.message}`);
  }
}

// /providers command
async function handleProviders(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const settings = await getUserSettings(userId);
  const providerButtons = Object.entries(PROVIDERS).map(([key, provider]) => [{
    text: `${key === settings.provider ? '✅ ' : ''}${provider.name}`,
    callback_data: `set_provider_${key}`,
  }]);
  
  const text = `<b>🔌 Select AI Provider:</b>\n\n<i>Current: ${PROVIDERS[settings.provider]?.name || settings.provider}</i>`;
  return telegram.sendInlineButtons(chatId, text, providerButtons);
}

// /models command
async function handleModels(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  const settings = await getUserSettings(userId);
  const provider = PROVIDERS[settings.provider];
  
  if (!provider) {
    return telegram.sendMessage(chatId, 'Invalid provider selected.');
  }
  
  await telegram.sendMessage(chatId, `<i>Loading ${provider.name} models...</i>`);
  
  const models = await fetchModels(settings.provider);
  
  if (models.length === 0) {
    return telegram.sendMessage(chatId, 'No models available for this provider.');
  }
  
  const modelButtons = models.map(m => [{
    text: `${m.id === settings.model ? '✅ ' : ''}${m.name}`,
    callback_data: `set_model_full_${m.id}`,
  }]);
  
  const text = `<b>📋 ${provider.name} Models:</b>\n\n<i>Current: ${settings.model}</i>`;
  return telegram.sendInlineButtons(chatId, text, modelButtons);
}

// /ai command
async function handleAI(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const prompt = (message.text || '').replace('/ai', '').trim();
  
  if (!prompt) {
    return telegram.sendMessage(chatId, 'Please provide a prompt. Example: /ai What is the moon?');
  }
  
  return processAIRequest(chatId, userId, prompt);
}

// Process AI request with chat history
export async function processAIRequest(chatId, userId, prompt) {
  const settings = await getUserSettings(userId);
  const provider = PROVIDERS[settings.provider];
  
  if (!provider) {
    return telegram.sendMessage(chatId, 'Invalid provider selected.');
  }
  
  // Get or create active chat
  const activeChat = await getActiveChat(userId);
  
  // Save user message
  await saveMessage(activeChat.id, 'user', prompt);
  
  // Auto-generate title from first message
  const messageCount = await getMessageCount(activeChat.id);
  if (messageCount === 1 && activeChat.title === 'New Chat') {
    const shortTitle = prompt.substring(0, 50) + (prompt.length > 50 ? '...' : '');
    await renameChat(activeChat.id, shortTitle);
  }
  
  // Send typing indicator
  await telegram.sendChatAction(chatId, 'typing');
  
  // Send status message that we'll update
  const statusMsg = await telegram.sendHtmlMessage(chatId, `🤔 <i>Thinking...</i>\n\n<code>${provider.name}: ${settings.model}</code>`);
  const statusMsgId = statusMsg?.result?.message_id;
  
  // Keep sending typing action while processing
  const typingInterval = setInterval(() => {
    telegram.sendChatAction(chatId, 'typing');
  }, 4000);
  
  try {
    // Build context: summary + recent messages
    const recentMessages = await getChatMessages(activeChat.id, 4);
    const history = recentMessages.reverse().map(m => ({
      role: m.role,
      content: m.content,
    }));
    
    // Add summary as system context if available
    const contextPrefix = activeChat.summary 
      ? `[Previous conversation summary: ${activeChat.summary}]\n\n` 
      : '';
    
    // Update status
    if (statusMsgId) {
      await telegram.editMessageText(chatId, statusMsgId, `⏳ <i>Processing...</i>\n\n<code>${provider.name}: ${settings.model}</code>`);
    }
    
    // Create timeout wrapper (60 seconds)
    const timeoutMs = 60000;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    let response;
    try {
      switch (settings.provider) {
        case 'gemini':
          response = await callGemini(prompt, settings.model, history, contextPrefix);
          break;
        case 'openai':
          response = await callOpenAI(prompt, settings.model, history, contextPrefix);
          break;
        case 'claude':
          response = await callClaude(prompt, settings.model, history, contextPrefix);
          break;
        case 'groq':
          response = await callGroq(prompt, settings.model, history, contextPrefix);
          break;
        default:
          clearInterval(typingInterval);
          clearTimeout(timeoutId);
          return telegram.sendMessage(chatId, 'Unknown provider.');
      }
    } finally {
      clearTimeout(timeoutId);
    }
    
    clearInterval(typingInterval);
    
    // Delete status message
    if (statusMsgId) {
      try {
        await telegram.editMessageText(chatId, statusMsgId, `✅ <i>Done!</i>`, 'HTML');
      } catch (e) { /* ignore */ }
    }
    
    // Save assistant response
    if (response.content) {
      await saveMessage(activeChat.id, 'assistant', response.content);
      
      // Update summary every 6 messages
      const newMessageCount = await getMessageCount(activeChat.id);
      if (newMessageCount % 6 === 0) {
        updateConversationSummary(activeChat.id, settings);
      }
    }
    
    // Send thinking process if available
    if (response.thinking) {
      const thinkingHtml = `<b>💭 Thinking:</b>\n<i>${telegram.escapeHtml(response.thinking.substring(0, 1000))}${response.thinking.length > 1000 ? '...' : ''}</i>`;
      await telegram.sendLongHtmlMessage(chatId, thinkingHtml);
    }
    
    // Send the main response
    if (response.content) {
      const responseHtml = `<b>💬 ${provider.name}:</b>\n${telegram.markdownToHtml(response.content)}`;
      await telegram.sendLongHtmlMessage(chatId, responseHtml);
    } else {
      await telegram.sendMessage(chatId, '⚠️ No response from AI. Try a different model or provider.');
    }
  } catch (error) {
    clearInterval(typingInterval);
    
    // Update status to show error
    if (statusMsgId) {
      try {
        await telegram.editMessageText(chatId, statusMsgId, `❌ <i>Error occurred</i>`, 'HTML');
      } catch (e) { /* ignore */ }
    }
    
    // Detailed error message
    const errorMessage = error.name === 'AbortError' 
      ? '⏱️ Request timed out (60s). The model may be overloaded. Try again or switch to a faster model.'
      : `❌ Error: ${error.message}\n\n<i>Try /models to switch models or /providers to change provider.</i>`;
    
    await telegram.sendHtmlMessage(chatId, errorMessage);
    console.error('AI request error:', error);
  }
}

// Update conversation summary in background
async function updateConversationSummary(chatId, settings) {
  try {
    const chat = await getChat(chatId);
    if (!chat || chat.messages.length < 4) return;
    
    // Create prompt for summarization
    const messagesText = chat.messages
      .map(m => `${m.role}: ${m.content}`)
      .join('\n');
    
    const summaryPrompt = `Summarize this conversation in 2-3 sentences, capturing the key topics and context:\n\n${messagesText}`;
    
    let response;
    switch (settings.provider) {
      case 'gemini':
        response = await callGemini(summaryPrompt, settings.model);
        break;
      case 'openai':
        response = await callOpenAI(summaryPrompt, settings.model);
        break;
      case 'claude':
        response = await callClaude(summaryPrompt, settings.model);
        break;
    }
    
    if (response?.content) {
      await updateChatSummary(chatId, response.content);
    }
  } catch (error) {
    console.error('Failed to update summary:', error);
  }
}
