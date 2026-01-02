import Redis from 'ioredis';
import { config } from '../config.js';

let redis = null;

export async function initCache() {
  try {
    redis = new Redis(config.redisUrl);
    
    redis.on('error', (err) => {
      console.error('Redis error:', err);
    });
    
    await redis.ping();
    console.log('✅ Redis connected');
  } catch (error) {
    console.error('❌ Redis connection failed:', error);
    throw error;
  }
}

export function getRedis() {
  if (!redis) {
    throw new Error('Redis not initialized');
  }
  return redis;
}

// Cache helpers with TTL (time to live in seconds)
export async function cacheGet(key) {
  return redis?.get(key);
}

export async function cacheSet(key, value, ttlSeconds = 3600) {
  return redis?.set(key, value, 'EX', ttlSeconds);
}

export async function cacheDelete(key) {
  return redis?.del(key);
}

// Rate limiting helper
export async function checkRateLimit(userId, limit = 10, windowSeconds = 60) {
  const key = `ratelimit:${userId}`;
  const current = await redis?.incr(key);
  
  if (current === 1) {
    await redis?.expire(key, windowSeconds);
  }
  
  return current <= limit;
}
