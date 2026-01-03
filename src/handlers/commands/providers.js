import * as telegram from '../../telegram/api.js';
import { PROVIDERS, fetchModels } from '../../providers/index.js';
import { getUserSettings } from '../../db/index.js';

// /providers command
export async function handleProviders(message) {
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
export async function handleModels(message) {
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
