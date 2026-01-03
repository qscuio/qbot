import * as telegram from '../../telegram/api.js';
import { config } from '../../config.js';
import { exportChatToGit } from '../../services/export.js';

// /export - Export chat to git
export async function handleExport(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  if (!config.notesRepo) {
    return telegram.sendMessage(chatId, '❌ Notes repo not configured. Set NOTES_REPO environment variable.');
  }
  
  await telegram.sendMessage(chatId, '📝 Exporting chat...');
  
  try {
    const result = await exportChatToGit(userId);
    
    const rawLink = result.rawUrl 
      ? `<a href="${result.rawUrl}">${result.rawFile}</a>`
      : result.rawFile;
    const notesLink = result.notesUrl 
      ? `<a href="${result.notesUrl}">${result.notesFile}</a>`
      : result.notesFile;
      
    return telegram.sendHtmlMessage(chatId, `✅ Chat exported!\n\n📄 Raw: ${rawLink}\n📝 Notes: ${notesLink}`);
  } catch (error) {
    return telegram.sendMessage(chatId, `❌ Export failed: ${error.message}`);
  }
}
