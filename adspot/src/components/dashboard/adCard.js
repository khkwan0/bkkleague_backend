import { useState } from 'react';
import ImageModal from '../common/ImageModal';

export default function AdCard({ad, toggleAdStatus, onUpdate}) {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const [error, setError] = useState(null);
  const [editData, setEditData] = useState({
    title: ad.title,
    message: ad.message,
    click_url: ad.click_url
  });
  const [adImage, setAdImage] = useState(`/ads/${ad.account_id}/${ad.id}/ad.png`);

  const handleImageUpdate = () => {
    // Force a refresh of the image by adding a timestamp
    const timestamp = new Date().getTime();
    const updatedImageUrl = `${adImage}?t=${timestamp}`;
    // Update the ad object with the new image URL
    setAdImage(updatedImageUrl);
  };

  const handleImageUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setIsUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('image', file);

      const response = await fetch(`/api/ads/${ad.id}/update-image`, {
        method: 'POST',
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to update image');
      }

      handleImageUpdate();
    } catch (err) {
      setError(err.message);
    } finally {
      setIsUploading(false);
    }
  };

  const handleEditChange = (e) => {
    const { name, value } = e.target;
    setEditData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSave = async () => {
    setError(null);
    try {
      const response = await fetch(`/api/ads/${ad.id}/update`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(editData),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to update ad');
      }

      if (onUpdate) {
        onUpdate(data.ad);
      }
      setIsEditing(false);
    } catch (err) {
      setError(err.message);
    }
  };
  
  return (
    <div 
      key={ad.id} 
      className="bg-white rounded-lg shadow-md p-6 hover:shadow-lg transition-shadow"
    >
      {/* Title */}
      <div className="flex justify-between items-start mb-4">
        {isEditing ? (
          <div>
            <input
              type="text"
              name="title"
              maxLength={50}
              value={editData.title}
              onChange={handleEditChange}
              className="text-xl font-semibold text-gray-900 border rounded px-2 py-1 w-full"
            />
            <div className="text-sm text-gray-500">Title (required): {50 - editData.title.length} characters remaining</div>
          </div>
        ) : (
          <h2 className="text-xl font-semibold text-gray-900">{ad.title}</h2>
        )}

        {/* Status Toggle */}
        <div className="flex items-center space-x-2 ml-4">
          <button
            onClick={() => toggleAdStatus(ad.id, ad.is_active)}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
              ad.is_active ? 'bg-blue-600' : 'bg-gray-200'
            }`}
          >
            <span
              className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                ad.is_active ? 'translate-x-6' : 'translate-x-1'
              }`}
            />
          </button>
          <span className={`px-3 py-1 rounded-full text-sm ${
            ad.is_active 
              ? "bg-green-100 text-green-800" 
              : "bg-red-100 text-red-800"
          }`}>
            {ad.is_active ? "Active" : "Inactive"}
          </span>
        </div>
      </div>
      
      {/* Message */}
      {isEditing ? (
        <div>
          <textarea
            name="message"
            maxLength={100}
            value={editData.message}
            onChange={handleEditChange}
            className="text-gray-600 w-full border rounded px-2 py-1 h-24"
          />
          <div className="text-sm text-gray-500">Message (optional): {100 - editData.message.length} characters remaining</div>
        </div>
      ) : (
        <p className="text-gray-600 mb-4 line-clamp-2 whitespace-pre-wrap">{ad.message}</p>
      )}

      {/* Image */}
      <div className="mb-4">
        <img 
          src={adImage} 
          alt={'ad image'} 
          className="w-64 h-64 object-contain rounded-lg cursor-pointer hover:opacity-90 transition-opacity"
          onClick={() => setIsModalOpen(true)}
        />
        {isEditing && (
          <label className="mt-2 inline-block bg-blue-600 text-white px-4 py-2 rounded-lg cursor-pointer hover:bg-blue-700 transition-colors text-sm font-medium">
            <svg className="w-4 h-4 inline-block mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
            {isUploading ? 'Uploading...' : 'Change Image'}
            <input
              type="file"
              accept="image/*"
              onChange={handleImageUpload}
              className="hidden"
              disabled={isUploading}
            />
          </label>
        )}
      </div>
      
      {/* Details */}
      <div className="space-y-2 text-sm text-gray-500 mb-4">
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
          </svg>
          {isEditing ? (
            <input
              type="text"
              name="click_url"
              maxLength={80}
              value={editData.click_url}
              onChange={handleEditChange}
              className="flex-1 border rounded px-2 py-1"
            />
          ) : (
            <a href={ad.click_url} className="hover:text-blue-600 truncate" target="_blank" rel="noopener noreferrer">
              {ad.click_url}
            </a>
          )}
        </div>
        
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          Created: {ad.created_at.toLocaleString()}
        </div>
        
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Updated: {ad.updated_at.toLocaleString()}
        </div>
      </div>

      {/* Edit Controls */}
      <div className="flex justify-end items-center space-x-2 pt-2 border-t border-gray-100">
        {isEditing ? (
          <>
            <button
              onClick={handleSave}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
            >
              Save Changes
            </button>
            <button
              onClick={() => {
                setIsEditing(false);
                setEditData({
                  title: ad.title,
                  message: ad.message,
                  click_url: ad.click_url
                });
              }}
              className="px-4 py-2 bg-gray-500 text-white rounded-lg hover:bg-gray-600 transition-colors"
            >
              Cancel
            </button>
          </>
        ) : (
          <button
            onClick={() => setIsEditing(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Edit Ad
          </button>
        )}
      </div>

      {error && (
        <div className="mt-2 text-red-500 text-sm text-center">
          {error}
        </div>
      )}

      <ImageModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        imageUrl={adImage}
        alt="Ad image"
      />
    </div>
  );
}