import * as telegram from '../telegram/api.js';
import { PROVIDERS } from '../providers/index.js';
import { 
  getUserSettings, 
  setUserProvider, 
  setUserModel,
  setActiveChat,
  deleteChat,
  getChat,
} from '../db/index.js';
import { processAIRequest } from './message.js';

// Callback query handler (inline button presses)
export async function handleCallbackQuery(callbackQuery) {
  const chatId = callbackQuery.message.chat.id;
  const userId = callbackQuery.from?.id;
  const data = callbackQuery.data;
  
  // Ask AI button
  if (data === 'ask_ai') {
    await telegram.answerCallbackQuery(callbackQuery.id, 'Asking AI...');
    return processAIRequest(chatId, userId, 'Tell me a random fun fact.');
  }
  
  // Switch chat: switch_chat_{chatId}
  if (data.startsWith('switch_chat_')) {
    const targetChatId = data.replace('switch_chat_', '');
    const chat = await getChat(targetChatId);
    
    if (chat && chat.userId === BigInt(userId)) {
      await setActiveChat(userId, targetChatId);
      await telegram.answerCallbackQuery(callbackQuery.id, 'Chat switched!');
      
      // Show chat title
      let response = `✅ Switched to: <b>${telegram.escapeHtml(chat.title)}</b>\n`;
      
      // Show recent messages if any
      if (chat.messages && chat.messages.length > 0) {
        const recent = chat.messages.slice(-5); // Last 5 messages
        response += `\n<i>Recent messages:</i>\n`;
        for (const msg of recent) {
          const icon = msg.role === 'user' ? '👤' : '🤖';
          const content = msg.content.substring(0, 100) + (msg.content.length > 100 ? '...' : '');
          response += `\n${icon} ${telegram.escapeHtml(content)}`;
        }
        response += `\n\n<i>Send a message to continue...</i>`;
      } else {
        response += `\n<i>This chat is empty. Send a message to start!</i>`;
      }
      
      return telegram.sendMessage(chatId, response);
    }
    return telegram.answerCallbackQuery(callbackQuery.id, 'Chat not found.');
  }
  
  // Delete chat: delete_chat_{chatId}
  if (data.startsWith('delete_chat_')) {
    const targetChatId = data.replace('delete_chat_', '');
    const chat = await getChat(targetChatId);
    
    if (chat && chat.userId === BigInt(userId)) {
      await deleteChat(targetChatId);
      await telegram.answerCallbackQuery(callbackQuery.id, 'Chat deleted!');
      return telegram.sendMessage(chatId, `🗑️ Deleted: <b>${telegram.escapeHtml(chat.title)}</b>`);
    }
    return telegram.answerCallbackQuery(callbackQuery.id, 'Chat not found.');
  }
  
  // Provider selection: set_provider_gemini, set_provider_openai, etc.
  if (data.startsWith('set_provider_')) {
    const providerKey = data.replace('set_provider_', '');
    const provider = PROVIDERS[providerKey];
    
    if (provider) {
      await setUserProvider(userId, providerKey, provider.defaultModel);
      await telegram.answerCallbackQuery(callbackQuery.id, `Provider set to ${provider.name}!`);
      return telegram.sendHtmlMessage(chatId, `✅ Provider set to <b>${provider.name}</b>\n<i>Model reset to ${provider.defaultModel}</i>`);
    }
    return telegram.answerCallbackQuery(callbackQuery.id, 'Unknown provider.');
  }
  
  // Model selection: set_model_flash, set_model_gpt-4o, etc.
  if (data.startsWith('set_model_')) {
    const modelShortName = data.replace('set_model_', '');
    const settings = await getUserSettings(userId);
    const provider = PROVIDERS[settings.provider];
    
    if (provider && provider.models[modelShortName]) {
      await setUserModel(userId, provider.models[modelShortName]);
      await telegram.answerCallbackQuery(callbackQuery.id, `Model set to ${modelShortName}!`);
      return telegram.sendHtmlMessage(chatId, `✅ Model set to <b>${provider.models[modelShortName]}</b>`);
    }
    return telegram.answerCallbackQuery(callbackQuery.id, 'Unknown model.');
  }
  
  // Unknown callback - acknowledge anyway
  return telegram.answerCallbackQuery(callbackQuery.id, 'Button press acknowledged!');
}
