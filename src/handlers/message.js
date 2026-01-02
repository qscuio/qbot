import * as telegram from '../telegram/api.js';
import { PROVIDERS, DEFAULT_PROVIDER } from '../providers/index.js';
import { callGemini } from '../providers/gemini.js';
import { callOpenAI } from '../providers/openai.js';
import { callClaude } from '../providers/claude.js';
import { getUserSettings, setUserProvider, setUserModel } from '../db/index.js';

// Random reactions list
const REACTIONS = ['👍', '👎', '❤', '🔥', '🥰', '👏', '😁', '🤔', '🤯', '😱', '🤬', '😢', '🎉', '🤩', '🤮', '💩', '🙏', '👌', '🕊', '🤡', '🥱', '🥴', '😍', '🐳', '❤‍🔥', '🌚', '🌭', '💯', '🤣', '⚡', '🍌', '🏆', '💔', '🤨', '😐', '🍓', '🍾', '💋', '🖕', '😈', '😴', '😭', '🤓', '👻', '👨‍💻', '👀', '🎃', '🙈', '😇', '😨', '🤝', '✍', '🤗', '🫡', '🎅', '🎄', '☃', '💅', '🤪', '🗿', '🆒', '💘', '🙉', '🦄', '😘', '💊', '🙊', '😎', '👾', '🤷‍♂', '🤷', '🤷‍♀', '😡'];

// Command handlers registry - add new commands here
const commands = {
  '/start': handleStart,
  '/help': handleStart,
  '/ai': handleAI,
  '/providers': handleProviders,
  '/models': handleModels,
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
  const helpText = `<b>Functions:</b>
<code>/help</code> - This message
/ai &lt;text&gt; - Ask AI (uses your selected provider)
/providers - List/select AI providers
/models - List/select models for current provider
Any other text will trigger a random reaction!`;

  const buttons = [[{ text: '🤖 Ask AI (Fun Fact)', callback_data: 'ask_ai' }]];
  return telegram.sendInlineButtons(chatId, helpText, buttons);
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
  
  const modelButtons = Object.entries(provider.models).map(([shortName, fullName]) => [{
    text: `${fullName === settings.model ? '✅ ' : ''}${shortName}`,
    callback_data: `set_model_${shortName}`,
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

// Process AI request (shared by command and callback)
export async function processAIRequest(chatId, userId, prompt) {
  const settings = await getUserSettings(userId);
  const provider = PROVIDERS[settings.provider];
  
  if (!provider) {
    return telegram.sendMessage(chatId, 'Invalid provider selected.');
  }
  
  await telegram.sendMessage(chatId, `🤔 Thinking... (${provider.name}: ${settings.model})`);
  
  try {
    let response;
    switch (settings.provider) {
      case 'gemini':
        response = await callGemini(prompt, settings.model);
        break;
      case 'openai':
        response = await callOpenAI(prompt, settings.model);
        break;
      case 'claude':
        response = await callClaude(prompt, settings.model);
        break;
      default:
        return telegram.sendMessage(chatId, 'Unknown provider.');
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
      await telegram.sendMessage(chatId, 'No response from AI.');
    }
  } catch (error) {
    await telegram.sendMessage(chatId, `Error: ${error.message}`);
  }
}
