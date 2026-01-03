import * as telegram from '../telegram/api.js';
import { PROVIDERS } from '../providers/index.js';
import { 
  getUserSettings, 
  setUserProvider, 
  setUserModel,
  setActiveChat,
  deleteChat,
  getChat,
  createChat,
  clearChatMessages,
  updateChatSummary,
  getUserChats,
} from '../db/index.js';
import { processAIRequest } from './message.js';
import { config } from '../config.js';
import { exportChatToGit } from '../services/export.js';

// Callback query handler (inline button presses)
export async function handleCallbackQuery(callbackQuery) {
  const chatId = callbackQuery.message.chat.id;
  const userId = callbackQuery.from?.id;
  const data = callbackQuery.data;
  
  // Command buttons from /start menu
  if (data === 'cmd_new') {
    await telegram.answerCallbackQuery(callbackQuery.id, 'Creating new chat...');
    const chat = await createChat(userId);
    return telegram.sendHtmlMessage(chatId, `✨ <b>New chat created!</b>\n\nSend me a message to start chatting.`);
  }
  
  if (data === 'cmd_chats') {
    await telegram.answerCallbackQuery(callbackQuery.id);
    const settings = await getUserSettings(userId);
    const chats = await getUserChats(userId, 10);
    
    if (chats.length === 0) {
      return telegram.sendMessage(chatId, 'No chats yet. Send a message to start!');
    }
    
    const buttons = chats.map(chat => [{
      text: `${chat.id === settings.activeChatId ? '✅ ' : ''}${chat.title.substring(0, 30)}`,
      callback_data: `switch_chat_${chat.id}`,
    }]);
    
    return telegram.sendInlineButtons(chatId, `<b>📂 Your Chats:</b>\n\n<i>Tap to switch</i>`, buttons);
  }
  
  if (data === 'cmd_providers') {
    await telegram.answerCallbackQuery(callbackQuery.id);
    const settings = await getUserSettings(userId);
    const providerButtons = Object.entries(PROVIDERS).map(([key, provider]) => [{
      text: `${key === settings.provider ? '✅ ' : ''}${provider.name}`,
      callback_data: `set_provider_${key}`,
    }]);
    
    const text = `<b>🔌 Select AI Provider:</b>\n\n<i>Current: ${PROVIDERS[settings.provider]?.name || settings.provider}</i>`;
    return telegram.sendInlineButtons(chatId, text, providerButtons);
  }
  
  if (data === 'cmd_models') {
    await telegram.answerCallbackQuery(callbackQuery.id);
    // Trigger /models command flow
    const { fetchModels } = await import('../providers/index.js');
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
    
    return telegram.sendInlineButtons(chatId, `<b>📋 ${provider.name} Models:</b>\n\n<i>Current: ${settings.model}</i>`, modelButtons);
  }
  
  if (data === 'cmd_export') {
    await telegram.answerCallbackQuery(callbackQuery.id, 'Exporting...');
    
    if (!config.notesRepo) {
      return telegram.sendMessage(chatId, '❌ Notes repo not configured. Set NOTES_REPO secret.');
    }
    
    await telegram.sendMessage(chatId, '📝 Exporting chat...');
    
    try {
      const result = await exportChatToGit(userId);
      return telegram.sendMessage(chatId, `✅ Chat exported!\n\n📄 ${result.filename}`);
    } catch (error) {
      return telegram.sendMessage(chatId, `❌ Export failed: ${error.message}`);
    }
  }
  
  if (data === 'cmd_clear') {
    await telegram.answerCallbackQuery(callbackQuery.id, 'Clearing...');
    const { getActiveChat } = await import('../db/index.js');
    const activeChat = await getActiveChat(userId);
    await clearChatMessages(activeChat.id);
    await updateChatSummary(activeChat.id, null);
    return telegram.sendMessage(chatId, '🗑️ Chat history cleared!');
  }
  
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
      response += `<i>${chat.messages?.length || 0} messages</i>\n`;
      
      // Show all messages
      if (chat.messages && chat.messages.length > 0) {
        response += `\n<b>Chat History:</b>\n`;
        for (const msg of chat.messages) {
          const icon = msg.role === 'user' ? '👤' : '🤖';
          const content = msg.content.substring(0, 200) + (msg.content.length > 200 ? '...' : '');
          response += `\n${icon} ${telegram.escapeHtml(content)}\n`;
        }
        response += `\n<i>Send a message to continue...</i>`;
      } else {
        response += `\n<i>This chat is empty. Send a message to start!</i>`;
      }
      
      return telegram.sendLongHtmlMessage(chatId, response);
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
      return telegram.sendHtmlMessage(chatId, `🗑️ Deleted: <b>${telegram.escapeHtml(chat.title)}</b>`);
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
  
  // Model selection with full ID: set_model_full_{fullModelId}
  if (data.startsWith('set_model_full_')) {
    const modelId = data.replace('set_model_full_', '');
    await setUserModel(userId, modelId);
    await telegram.answerCallbackQuery(callbackQuery.id, 'Model set!');
    return telegram.sendHtmlMessage(chatId, `✅ Model set to <b>${modelId}</b>`);
  }
  
  // Legacy model selection: set_model_{shortName}
  if (data.startsWith('set_model_')) {
    const modelShortName = data.replace('set_model_', '');
    const settings = await getUserSettings(userId);
    const provider = PROVIDERS[settings.provider];
    
    if (provider && provider.fallbackModels && provider.fallbackModels[modelShortName]) {
      await setUserModel(userId, provider.fallbackModels[modelShortName]);
      await telegram.answerCallbackQuery(callbackQuery.id, `Model set to ${modelShortName}!`);
      return telegram.sendHtmlMessage(chatId, `✅ Model set to <b>${provider.fallbackModels[modelShortName]}</b>`);
    }
    return telegram.answerCallbackQuery(callbackQuery.id, 'Unknown model.');
  }
  
  // Unknown callback - acknowledge anyway
  return telegram.answerCallbackQuery(callbackQuery.id, 'Button press acknowledged!');
}
