import { config } from '../config.js';

const TIMEOUT_MS = 55000;

export async function callClaude(prompt, model, history = [], contextPrefix = '') {
  if (!config.claudeApiKey) {
    throw new Error('CLAUDE_API_KEY is not set');
  }
  
  // Build messages array with history
  const messages = [];
  
  // Add history messages
  for (const msg of history) {
    messages.push({
      role: msg.role,
      content: msg.content,
    });
  }
  
  // Add current prompt with context prefix
  const fullPrompt = contextPrefix ? `${contextPrefix}${prompt}` : prompt;
  messages.push({ role: 'user', content: fullPrompt });
  
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  
  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': config.claudeApiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model,
        max_tokens: 4096,
        messages,
      }),
      signal: controller.signal,
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Claude API Error: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    let thinking = '', content = '';
    
    // Claude can return thinking blocks
    if (data.content) {
      for (const block of data.content) {
        if (block.type === 'thinking') thinking += block.thinking || '';
        else if (block.type === 'text') content += block.text || '';
      }
    }
    
    return { thinking, content };
  } finally {
    clearTimeout(timeoutId);
  }
}
