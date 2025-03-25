import { cookies } from 'next/headers';
import { get } from './cache';

/**
 * Get the current user's session data
 * @returns {Promise<{userId: number} | null>} The session data if found, null otherwise
 */
export async function getSession() {
  try {
    const cookieStore = await cookies();
    const sessionToken = cookieStore.get('session')?.value;

    if (!sessionToken) {
      return null;
    }

    const sessionData = await get(`session:${sessionToken}`);
    return sessionData;
  } catch (error) {
    console.error('Error getting session:', error);
    return null;
  }
}
