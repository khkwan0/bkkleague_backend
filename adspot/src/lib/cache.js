import {createClient} from "redis";

class RedisError extends Error {
  constructor(message, operation, originalError) {
    super(message);
    this.name = 'RedisError';
    this.operation = operation;
    this.originalError = originalError;
  }
}

const redis = createClient({
  url: process.env.REDIS_URL,
})

redis.on('error', (err) => console.error('Redis Client Error', err));

;(async () => {
  await redis.connect()
})()

/**
 * Get a value from Redis by key
 * @param {string} key - The key to retrieve from Redis
 * @returns {Promise<any>} The value stored at the key, or null if not found
 */
export async function get(key) {
  try {
    const value = await redis.get(key);
    return value ? JSON.parse(value) : null;
  } catch (error) {
    console.error('Error getting value from Redis:', error);
    return null;
  }
}

/**
 * Set a key-value pair in Redis with optional TTL
 * @param {string} key - The key to store the value under
 * @param {any} value - The value to store
 * @param {number} [ttlSeconds] - Optional time to live in seconds
 * @returns {Promise<boolean>} True if successful, false otherwise
 * @throws {RedisError} When validation fails or Redis operation fails
 */
export async function set(key, value, ttlSeconds) {
  // Input validation
  if (!key || typeof key !== 'string') {
    throw new RedisError('Key must be a non-empty string', 'set', null);
  }

  if (value === undefined || value === null) {
    throw new RedisError('Value cannot be undefined or null', 'set', null);
  }

  if (ttlSeconds !== undefined && (typeof ttlSeconds !== 'number' || ttlSeconds <= 0)) {
    throw new RedisError('TTL must be a positive number', 'set', null);
  }

  try {
    const stringValue = JSON.stringify(value);
    
    if (ttlSeconds) {
      await redis.set(key, stringValue, {EX: ttlSeconds});
    } else {
      await redis.set(key, stringValue);
    }
    return true;
  } catch (error) {
    const redisError = new RedisError(
      `Failed to set value in Redis: ${error.message}`,
      'set',
      error
    );
    console.error(redisError);
    throw redisError;
  }
}

/**
 * Delete a key from Redis
 * @param {string} key - The key to delete from Redis
 * @returns {Promise<boolean>} True if successful, false otherwise
 * @throws {RedisError} When validation fails or Redis operation fails
 */
export async function del(key) {
  // Input validation
  if (!key || typeof key !== 'string') {
    throw new RedisError('Key must be a non-empty string', 'del', null);
  }

  try {
    await redis.del(key);
    return true;
  } catch (error) {
    const redisError = new RedisError(
      `Failed to delete key from Redis: ${error.message}`,
      'del',
      error
    );
    console.error(redisError);
    throw redisError;
  }
}