import * as telegram from '../../telegram/api.js';
import { config } from '../../config.js';
import { 
  getAllowedUsers,
  addAllowedUser,
  removeAllowedUser,
} from '../../db/index.js';

// /adduser - Add user to allowed list (owner only)
export async function handleAddUser(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const targetId = (message.text || '').replace('/adduser', '').trim();
  
  if (String(userId) !== config.ownerId) {
    return telegram.sendMessage(chatId, '🚫 Only the owner can add users.');
  }
  
  if (!targetId || isNaN(Number(targetId))) {
    return telegram.sendMessage(chatId, 'Usage: /adduser <user_id>');
  }
  
  await addAllowedUser(targetId, userId);
  return telegram.sendMessage(chatId, `✅ User ${targetId} added to allowed list.`);
}

// /deluser - Remove user from allowed list (owner only)
export async function handleDelUser(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  const targetId = (message.text || '').replace('/deluser', '').trim();
  
  if (String(userId) !== config.ownerId) {
    return telegram.sendMessage(chatId, '🚫 Only the owner can remove users.');
  }
  
  if (!targetId || isNaN(Number(targetId))) {
    return telegram.sendMessage(chatId, 'Usage: /deluser <user_id>');
  }
  
  // Prevent deleting owner
  if (targetId === config.ownerId) {
    return telegram.sendMessage(chatId, '🚫 Cannot delete owner.');
  }
  
  // Prevent deleting env-configured users
  if (config.allowedUsers.includes(targetId)) {
    return telegram.sendMessage(chatId, '⚠️ Cannot delete users configured in environment. Remove from ALLOWED_USERS env var.');
  }
  
  await removeAllowedUser(targetId);
  return telegram.sendMessage(chatId, `✅ User ${targetId} removed from allowed list.`);
}

// /users - List all allowed users (owner only)
export async function handleUsers(message) {
  const chatId = message.chat.id;
  const userId = message.from?.id;
  
  if (String(userId) !== config.ownerId) {
    return telegram.sendMessage(chatId, '🚫 Only the owner can view users.');
  }
  
  const dbUsers = await getAllowedUsers();
  
  let response = '<b>👥 Allowed Users:</b>\n\n';
  response += '<b>Environment (ALLOWED_USERS):</b>\n';
  config.allowedUsers.forEach((id, i) => {
    const isOwner = i === 0 ? ' 👑' : '';
    response += `• ${id}${isOwner}\n`;
  });
  
  if (dbUsers.length > 0) {
    response += '\n<b>Database:</b>\n';
    dbUsers.forEach(u => {
      response += `• ${u.id.toString()}\n`;
    });
  }
  
  return telegram.sendHtmlMessage(chatId, response);
}
