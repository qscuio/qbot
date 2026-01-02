import { config } from '../config.js';

const TIMEOUT_MS = 55000;

export async function callOpenAI(prompt, model) {
  if (!config.openaiApiKey) {
    throw new Error('OPENAI_API_KEY is not set');
  }
  
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${config.openaiApiKey}`,
      },
      body: JSON.stringify({
        model,
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 4096,
      }),
      signal: controller.signal,
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenAI API Error: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    return { thinking: '', content: data.choices?.[0]?.message?.content || '' };
  } finally {
    clearTimeout(timeoutId);
  }
}
