import { config } from '../config.js';

const TIMEOUT_MS = 55000;

export async function callGemini(prompt, model, history = [], contextPrefix = '') {
  if (!config.geminiApiKey) {
    throw new Error('GEMINI_API_KEY is not set');
  }
  
  // Build contents array with history
  const contents = [];
  
  // Add history messages
  for (const msg of history) {
    contents.push({
      role: msg.role === 'assistant' ? 'model' : 'user',
      parts: [{ text: msg.content }],
    });
  }
  
  // Add current prompt with context prefix
  const fullPrompt = contextPrefix ? `${contextPrefix}${prompt}` : prompt;
  contents.push({ role: 'user', parts: [{ text: fullPrompt }] });
  
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${config.geminiApiKey}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents,
        generationConfig: { thinkingConfig: { thinkingBudget: 1024 } },
      }),
      signal: controller.signal,
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Gemini API Error: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    let thinking = '', content = '';
    
    if (data.candidates?.[0]?.content?.parts) {
      for (const part of data.candidates[0].content.parts) {
        if (part.thought) thinking += part.text || '';
        else content += part.text || '';
      }
    }
    
    return { thinking, content };
  } finally {
    clearTimeout(timeoutId);
  }
}
