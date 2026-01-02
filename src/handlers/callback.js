import * as telegram from '../telegram/api.js';
import { PROVIDERS } from '../providers/index.js';
import { getUserSettings, setUserProvider, setUserModel } from '../db/index.js';
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
