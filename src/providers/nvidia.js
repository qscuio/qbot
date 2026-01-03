import { config } from '../config.js';

const TIMEOUT_MS = 55000;

export async function callNvidia(prompt, model, history = [], contextPrefix = '') {
  if (!config.nvidiaApiKey) {
    throw new Error('NVIDIA_API_KEY is not set');
  }
  
  // Build messages array with history
  const messages = [];
  
  // Add system message with context if available
  if (contextPrefix) {
    messages.push({ role: 'system', content: contextPrefix });
  }
  
  // Add history messages
  for (const msg of history) {
    messages.push({
      role: msg.role,
      content: msg.content,
    });
  }
  
  // Add current prompt
  messages.push({ role: 'user', content: prompt });
  
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  
  try {
    const response = await fetch('https://integrate.api.nvidia.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${config.nvidiaApiKey}`,
      },
      body: JSON.stringify({
        model,
        messages,
        max_tokens: 4096,
      }),
      signal: controller.signal,
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`NVIDIA API Error: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    return { thinking: '', content: data.choices?.[0]?.message?.content || '' };
  } finally {
    clearTimeout(timeoutId);
  }
}
