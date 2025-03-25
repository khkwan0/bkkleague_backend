export default function AdCard({ad, toggleAdStatus}) {
  const adImage = `/ads/${ad.account_id}/${ad.id}/ad.png`
  return (
    <div 
      key={ad.id} 
      className="bg-white rounded-lg shadow-md p-6 hover:shadow-lg transition-shadow"
    >
      <div className="flex justify-between items-start mb-4">
        <h2 className="text-xl font-semibold text-gray-900">{ad.title}</h2>
        <div className="flex items-center space-x-2">
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
      
      <p className="text-gray-600 mb-4 line-clamp-2">{ad.message}</p>

      <img src={adImage} alt={'ad image'} className="w-32 h-32 object-cover rounded-lg mb-4" />
      
      <div className="space-y-2 text-sm text-gray-500">
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
          </svg>
          <a href={ad.click_url} className="hover:text-blue-600 truncate" target="_blank" rel="noopener noreferrer">
            {ad.click_url}
          </a>
        </div>
        
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          Created: {ad.created_at.toLocaleString()}
        </div>
        
        <div className="flex items-center">
          <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Updated: {ad.updated_at.toLocaleString()}
        </div>
      </div>
    </div>
  );
}