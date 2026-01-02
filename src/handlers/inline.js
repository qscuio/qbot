import * as telegram from '../telegram/api.js';

// Inline query handler (for voice files, etc.)
export async function handleInlineQuery(inlineQuery) {
  // Placeholder for inline query handling
  // Can be extended to support voice files, stickers, etc.
  const results = [];
  return telegram.answerInlineQuery(inlineQuery.id, results);
}
