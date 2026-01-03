import { config } from '../config.js';

// Available AI providers configuration (with fallback models)
export const PROVIDERS = {
  groq: {
    name: 'Groq',
    fallbackModels: {
      'llama-70b': 'llama-3.3-70b-versatile',
      'llama-8b': 'llama-3.1-8b-instant',
      'mixtral': 'mixtral-8x7b-32768',
    },
    defaultModel: 'llama-3.3-70b-versatile',
  },
  gemini: {
    name: 'Gemini',
    fallbackModels: {
      'flash': 'gemini-2.0-flash',
      'flash-lite': 'gemini-2.0-flash-lite',
      'pro': 'gemini-2.5-pro-preview-06-05',
    },
    defaultModel: 'gemini-2.0-flash',
  },
  openai: {
    name: 'OpenAI',
    fallbackModels: {
      'gpt-4o': 'gpt-4o',
      'gpt-4o-mini': 'gpt-4o-mini',
      'gpt-4-turbo': 'gpt-4-turbo',
    },
    defaultModel: 'gpt-4o-mini',
  },
  claude: {
    name: 'Claude',
    fallbackModels: {
      'sonnet': 'claude-sonnet-4-20250514',
      'haiku': 'claude-3-5-haiku-20241022',
      'opus': 'claude-3-opus-20240229',
    },
    defaultModel: 'claude-sonnet-4-20250514',
  },
  nvidia: {
    name: 'NVIDIA',
    fallbackModels: {
      'llama-70b': 'meta/llama-3.1-70b-instruct',
      'llama-8b': 'meta/llama-3.1-8b-instruct',
      'mistral-large': 'mistralai/mistral-large-2-instruct',
    },
    defaultModel: 'meta/llama-3.1-70b-instruct',
  },
};

export const DEFAULT_PROVIDER = 'groq';

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

// Fetch models dynamically from provider API
export async function fetchModels(providerKey) {
  try {
    switch (providerKey) {
      case 'groq':
        return await fetchGroqModels();
      case 'gemini':
        return await fetchGeminiModels();
      case 'openai':
        return await fetchOpenAIModels();
      case 'claude':
        // Claude doesn't have a public models API, use fallback
        return getFallbackModels('claude');
      case 'nvidia':
        return await fetchNvidiaModels();
      default:
        return [];
    }
  } catch (error) {
    console.error(`Error fetching ${providerKey} models:`, error.message);
    return getFallbackModels(providerKey);
  }
}

// Get fallback models for a provider
function getFallbackModels(providerKey) {
  const provider = PROVIDERS[providerKey];
  if (!provider) return [];
  return Object.entries(provider.fallbackModels).map(([shortName, fullName]) => ({
    id: fullName,
    name: shortName,
  }));
}

// Fetch Groq models (OpenAI compatible API)
async function fetchGroqModels() {
  if (!config.groqApiKey) return getFallbackModels('groq');
  
  const response = await fetch('https://api.groq.com/openai/v1/models', {
    headers: { 'Authorization': `Bearer ${config.groqApiKey}` },
  });
  
  if (!response.ok) return getFallbackModels('groq');
  
  const data = await response.json();
  return (data.data || [])
    .filter(m => m.id && !m.id.includes('whisper'))
    .sort((a, b) => a.id.localeCompare(b.id))
    .map(m => ({ id: m.id, name: m.id }));
}

// Fetch Gemini models
async function fetchGeminiModels() {
  if (!config.geminiApiKey) return getFallbackModels('gemini');
  
  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models?key=${config.geminiApiKey}`
  );
  
  if (!response.ok) return getFallbackModels('gemini');
  
  const data = await response.json();
  return (data.models || [])
    .filter(m => m.name && m.supportedGenerationMethods?.includes('generateContent'))
    .map(m => ({
      id: m.name.replace('models/', ''),
      name: m.displayName || m.name.replace('models/', ''),
    }));
}

// Fetch OpenAI models
async function fetchOpenAIModels() {
  if (!config.openaiApiKey) return getFallbackModels('openai');
  
  const response = await fetch('https://api.openai.com/v1/models', {
    headers: { 'Authorization': `Bearer ${config.openaiApiKey}` },
  });
  
  if (!response.ok) return getFallbackModels('openai');
  
  const data = await response.json();
  return (data.data || [])
    .filter(m => m.id && (m.id.includes('gpt') || m.id.includes('o1') || m.id.includes('o3')))
    .filter(m => !m.id.includes('instruct') && !m.id.includes('realtime'))
    .sort((a, b) => b.id.localeCompare(a.id))
    .map(m => ({ id: m.id, name: m.id }));
}

// Fetch NVIDIA NIM models
async function fetchNvidiaModels() {
  if (!config.nvidiaApiKey) return getFallbackModels('nvidia');
  
  const response = await fetch('https://integrate.api.nvidia.com/v1/models', {
    headers: { 'Authorization': `Bearer ${config.nvidiaApiKey}` },
  });
  
  if (!response.ok) return getFallbackModels('nvidia');
  
  const data = await response.json();
  return (data.data || [])
    .filter(m => m.id && !m.id.includes('embed') && !m.id.includes('rerank'))
    .sort((a, b) => a.id.localeCompare(b.id))
    .map(m => ({ id: m.id, name: m.id }));
}

// Legacy function for backward compatibility
export function listModels(providerKey) {
  const provider = PROVIDERS[providerKey];
  if (!provider) return [];
  return Object.entries(provider.fallbackModels).map(([shortName, fullName]) => ({
    shortName,
    fullName,
  }));
}
