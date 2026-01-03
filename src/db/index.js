import { PrismaClient } from '@prisma/client';

export const prisma = new PrismaClient();

export async function initDatabase() {
  try {
    await prisma.$connect();
    console.log('✅ Database connected');
  } catch (error) {
    console.error('❌ Database connection failed:', error);
    throw error;
  }
}

// ============ User Functions ============

export async function getOrCreateUser(userId) {
  return prisma.user.upsert({
    where: { id: BigInt(userId) },
    update: {},
    create: { id: BigInt(userId) },
  });
}

export async function getUserSettings(userId) {
  const user = await getOrCreateUser(userId);
  return {
    provider: user.provider,
    model: user.model,
    activeChatId: user.activeChatId,
  };
}

export async function setUserProvider(userId, provider, defaultModel) {
  return prisma.user.upsert({
    where: { id: BigInt(userId) },
    update: { provider, model: defaultModel },
    create: { id: BigInt(userId), provider, model: defaultModel },
  });
}

export async function setUserModel(userId, model) {
  return prisma.user.upsert({
    where: { id: BigInt(userId) },
    update: { model },
    create: { id: BigInt(userId), model },
  });
}

// ============ Chat Functions ============

export async function createChat(userId, title = 'New Chat') {
  const chat = await prisma.chat.create({
    data: {
      userId: BigInt(userId),
      title,
    },
  });
  
  // Set as active chat
  await prisma.user.update({
    where: { id: BigInt(userId) },
    data: { activeChatId: chat.id },
  });
  
  return chat;
}

export async function getActiveChat(userId) {
  const user = await getOrCreateUser(userId);
  
  if (user.activeChatId) {
    const chat = await prisma.chat.findUnique({
      where: { id: user.activeChatId },
    });
    if (chat) return chat;
  }
  
  // No active chat - create one
  return createChat(userId);
}

export async function setActiveChat(userId, chatId) {
  return prisma.user.update({
    where: { id: BigInt(userId) },
    data: { activeChatId: chatId },
  });
}

export async function getUserChats(userId, limit = 10) {
  return prisma.chat.findMany({
    where: { userId: BigInt(userId) },
    orderBy: { updatedAt: 'desc' },
    take: limit,
  });
}

export async function getChat(chatId) {
  return prisma.chat.findUnique({
    where: { id: chatId },
    include: { messages: { orderBy: { createdAt: 'asc' } } },
  });
}

export async function renameChat(chatId, title) {
  return prisma.chat.update({
    where: { id: chatId },
    data: { title },
  });
}

export async function updateChatSummary(chatId, summary) {
  return prisma.chat.update({
    where: { id: chatId },
    data: { summary },
  });
}

export async function deleteChat(chatId) {
  return prisma.chat.delete({
    where: { id: chatId },
  });
}

export async function clearChatMessages(chatId) {
  return prisma.message.deleteMany({
    where: { chatId },
  });
}

// ============ Message Functions ============

export async function saveMessage(chatId, role, content) {
  // Update chat's updatedAt
  await prisma.chat.update({
    where: { id: chatId },
    data: { updatedAt: new Date() },
  });
  
  return prisma.message.create({
    data: { chatId, role, content },
  });
}

export async function getChatMessages(chatId, limit = 10) {
  return prisma.message.findMany({
    where: { chatId },
    orderBy: { createdAt: 'desc' },
    take: limit,
  });
}

export async function getMessageCount(chatId) {
  return prisma.message.count({
    where: { chatId },
  });
}
