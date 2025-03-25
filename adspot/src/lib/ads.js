import { query } from './db.js';

/**
 * Get ads from ad_spots table filtered by account_id
 * @param {string} accountId - The user's account ID
 * @returns {Promise<Array>} - Array of ad spots
 */
export async function getAdsByAccountId(accountId) {
  try {
    const ads = await query(
      'SELECT * FROM ad_spots WHERE account_id = ?',
      [accountId]
    );
    return ads;
  } catch (error) {
    console.error('Error fetching ads:', error);
    throw new Error('Failed to fetch ads');
  }
}

export async function updateAdStatus(id, is_active) {
  try {
    const updatedAd = await query(
      'UPDATE ad_spots SET is_active = ? WHERE id = ?',
      [is_active, id]
    );
    return updatedAd;
  } catch (error) {
    console.error('Error updating ad status:', error);
    throw new Error('Failed to update ad status');
  }
}

export async function getAdById(id) {
  try {
    const ad = await query('SELECT * FROM ad_spots WHERE id = ?', [id]);
    return ad[0];
  } catch (error) {
    console.error('Error fetching ad by id:', error);
    throw new Error('Failed to fetch ad by id');
  }
}

export async function createAd(accountId, title, message, click_url, is_active) {
  try {
    const ad = await query('INSERT INTO ad_spots (account_id, title, message, click_url, is_active) VALUES (?, ?, ?, ?, ?)', [accountId, title, message, click_url, is_active]);
    return ad.insertId;
  } catch (error) {
    console.error('Error creating ad:', error);
    throw new Error('Failed to create ad');
  }
}
