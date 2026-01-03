// Available AI providers configuration
export const PROVIDERS = {
  gemini: {
    name: 'Gemini',
    models: {
      'flash': 'gemini-2.0-flash',
      'flash-lite': 'gemini-2.0-flash-lite',
      'pro': 'gemini-2.5-pro-preview-06-05',
    },
    defaultModel: 'gemini-2.0-flash',
  },
  openai: {
    name: 'OpenAI',
    models: {
      'gpt-4o': 'gpt-4o',
      'gpt-4o-mini': 'gpt-4o-mini',
      'gpt-4-turbo': 'gpt-4-turbo',
    },
    defaultModel: 'gpt-4o-mini',
  },
  claude: {
    name: 'Claude',
    models: {
      'sonnet': 'claude-sonnet-4-20250514',
      'haiku': 'claude-3-5-haiku-20241022',
      'opus': 'claude-3-opus-20240229',
    },
    defaultModel: 'claude-sonnet-4-20250514',
  },
  groq: {
    name: 'Groq',
    models: {
      'llama-70b': 'llama-3.3-70b-versatile',
      'llama-8b': 'llama-3.1-8b-instant',
      'mixtral': 'mixtral-8x7b-32768',
    },
    defaultModel: 'llama-3.3-70b-versatile',
  },
};

export const DEFAULT_PROVIDER = 'gemini';

// Get provider info
export function getProvider(key) {
  return PROVIDERS[key] || null;
}

// List all providers
export function listProviders() {
  return Object.entries(PROVIDERS).map(([key, value]) => ({
    key,
    name: value.name,
    defaultModel: value.defaultModel,
  }));
}

// List models for a provider
export function listModels(providerKey) {
  const provider = PROVIDERS[providerKey];
  if (!provider) return [];
  return Object.entries(provider.models).map(([shortName, fullName]) => ({
    shortName,
    fullName,
  }));
}
