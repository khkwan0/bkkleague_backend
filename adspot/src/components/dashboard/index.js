'use client';
import { useState, useEffect } from 'react'; 
import AdCard from './adCard';

export default function DashboardComponent() {
  const [ads, setAds] = useState([]);

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

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <a 
          href="/dashboard/ads/new" 
          className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          Create New Ad
        </a>
      </div>

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
