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

// User settings helpers
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

// Conversation history (for future features)
export async function saveMessage(userId, role, content, provider, model) {
  return prisma.conversation.create({
    data: {
      userId: BigInt(userId),
      role,
      content,
      provider,
      model,
    },
  });
}

export async function getConversationHistory(userId, limit = 10) {
  return prisma.conversation.findMany({
    where: { userId: BigInt(userId) },
    orderBy: { createdAt: 'desc' },
    take: limit,
  });
}
