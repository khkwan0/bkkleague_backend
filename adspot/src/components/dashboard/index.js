'use client';
import { useState, useEffect } from 'react'; 
import AdCard from './adCard';
import { useRouter } from 'next/navigation';

export default function DashboardComponent() {
  const [ads, setAds] = useState([]);
  const [showPasswordModal, setShowPasswordModal] = useState(false);
  const [showPasswords, setShowPasswords] = useState(false);
  const [passwordForm, setPasswordForm] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  });
  const [passwordError, setPasswordError] = useState('');
  const [passwordSuccess, setPasswordSuccess] = useState('');
  const router = useRouter();

  useEffect(() => {
    const fetchAds = async () => {
      const response = await fetch('/api/ads');
      const data = await response.json();
      setAds(data);
    };
    fetchAds();
  }, []);

  const toggleAdStatus = async (adId, currentStatus) => {
    try {
      const response = await fetch(`/api/ads/${adId}/toggle-status`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ is_active: !currentStatus }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to toggle status');
      }

      const res = await response.json();
      if (res.status === 'ok') {
        setAds(ads.map(ad => ad.id === adId ? res.ad : ad));
      }
    } catch (error) {
      console.error('Error toggling ad status:', error);
      // You might want to add error handling UI here
    }
  };

  const handleAdUpdate = (updatedAd) => {
    setAds(ads.map(ad => ad.id === updatedAd.id ? updatedAd : ad));
  };

  const handleLogout = async () => {
    try {
      const response = await fetch('/api/auth/logout', {
        method: 'POST',
      });
      
      if (!response.ok) {
        throw new Error('Failed to logout');
      }

      // Redirect to login page after successful logout
      router.push('/auth/login');
    } catch (error) {
      console.error('Error logging out:', error);
      // You might want to add error handling UI here
    }
  };

  const handlePasswordChange = async (e) => {
    e.preventDefault();
    setPasswordError('');
    setPasswordSuccess('');

    if (passwordForm.newPassword !== passwordForm.confirmPassword) {
      setPasswordError('New passwords do not match');
      return;
    }

    try {
      const response = await fetch('/api/auth/change-password', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          currentPassword: passwordForm.currentPassword,
          newPassword: passwordForm.newPassword,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to change password');
      }

      setPasswordSuccess('Password changed successfully');
      setPasswordForm({
        currentPassword: '',
        newPassword: '',
        confirmPassword: ''
      });
      setTimeout(() => setShowPasswordModal(false), 2000);
    } catch (error) {
      setPasswordError(error.message);
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <div className="flex gap-4">
          <button
            onClick={() => setShowPasswordModal(true)}
            className="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            Change Password
          </button>
          <a 
            href="/dashboard/ads/new" 
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            Create New Ad
          </a>
          <button
            onClick={handleLogout}
            className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            Logout
          </button>
        </div>
      </div>

      {showPasswordModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center">
          <div className="bg-white p-6 rounded-lg shadow-xl max-w-md w-full">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-2xl font-bold">Change Password</h2>
              <button
                onClick={() => setShowPasswords(!showPasswords)}
                className="p-2 hover:bg-gray-100 rounded-full transition-colors"
                title={showPasswords ? "Hide passwords" : "Show passwords"}
              >
                {showPasswords ? (
                  <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6 text-gray-600">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.242 4.242L9.88 9.88" />
                  </svg>
                ) : (
                  <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6 text-gray-600">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                  </svg>
                )}
              </button>
            </div>
            <form onSubmit={handlePasswordChange}>
              <div className="mb-4">
                <label className="block text-gray-900 font-medium mb-2">Current Password</label>
                <input
                  type={showPasswords ? "text" : "password"}
                  value={passwordForm.currentPassword}
                  onChange={(e) => setPasswordForm({...passwordForm, currentPassword: e.target.value})}
                  className="w-full p-2 border rounded"
                  required
                />
              </div>
              <div className="mb-4">
                <label className="block text-gray-900 font-medium mb-2">New Password</label>
                <input
                  type={showPasswords ? "text" : "password"}
                  value={passwordForm.newPassword}
                  onChange={(e) => setPasswordForm({...passwordForm, newPassword: e.target.value})}
                  className="w-full p-2 border rounded"
                  required
                />
              </div>
              <div className="mb-4">
                <label className="block text-gray-900 font-medium mb-2">Confirm New Password</label>
                <input
                  type={showPasswords ? "text" : "password"}
                  value={passwordForm.confirmPassword}
                  onChange={(e) => setPasswordForm({...passwordForm, confirmPassword: e.target.value})}
                  className="w-full p-2 border rounded"
                  required
                />
              </div>
              {passwordError && (
                <div className="text-red-600 mb-4">{passwordError}</div>
              )}
              {passwordSuccess && (
                <div className="text-green-600 mb-4">{passwordSuccess}</div>
              )}
              <div className="flex justify-end gap-4">
                <button
                  type="button"
                  onClick={() => setShowPasswordModal(false)}
                  className="px-4 py-2 text-gray-600 hover:text-gray-800"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
                >
                  Change Password
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        {ads.map((ad) => (
          <AdCard 
            key={ad.id} 
            ad={ad} 
            toggleAdStatus={toggleAdStatus}
            onUpdate={handleAdUpdate}
          />
        ))}
      </div>
    </div>
  );
}
