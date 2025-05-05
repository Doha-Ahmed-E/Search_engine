import { useState, useEffect, useRef } from 'react';
import { Search, ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

const fullscreenStyle = {
  position: 'fixed',
  top: 0,
  left: 0,
  width: '100vw',
  height: '100vh',
  backgroundColor: '#0f172a',
  margin: 0,
  padding: 0,
  overflow: 'hidden',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
};

export default function SearchPage() {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchHistory, setSearchHistory] = useState(() => {
    if (typeof window !== 'undefined') {
      const storedHistory = localStorage.getItem('searchHistory');
      return storedHistory ? JSON.parse(storedHistory) : [];
    }
    return [];
  });
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);
  const navigate = useNavigate();
  
  useEffect(() => {
    localStorage.setItem('searchHistory', JSON.stringify(searchHistory));
  }, [searchHistory]);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  const handleSearch = (e) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      updateSearchHistory(searchQuery);
      navigate('/search-result', { 
        state: { 
          query: searchQuery,
        } 
      });
      setShowDropdown(false);
    }
  };

  const updateSearchHistory = (query) => {
    setSearchHistory(prevHistory => {
      const updatedHistory = prevHistory.filter(item => item !== query);
      updatedHistory.unshift(query);
      return updatedHistory.slice(0, 10);
    });
  };

  const handleHistoryClick = (query) => {
    setSearchQuery(query);
    updateSearchHistory(query);
    setShowDropdown(false);
    navigate('/search-result', { 
      state: { 
        query: query,
      } 
    });
  };

  return (
    <div style={fullscreenStyle}>
      {/* Adjusted the margin to position the logo higher */}
      <div className="mt-16 mb-8">
        <h1 className="text-6xl font-bold text-blue-400">
          Search<span className="text-purple-400">Now</span>
        </h1>
      </div>

      {/* Main search container with adjusted positioning */}
      <div className="w-11/12 max-w-2xl relative" ref={dropdownRef} style={{ marginTop: '2rem' }}>
        <form onSubmit={handleSearch} className="relative">
          <div className="flex items-center bg-white rounded-full shadow-xl overflow-hidden hover:shadow-2xl transition-shadow duration-200">
            <div className="pl-4">
              <Search className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onFocus={() => setShowDropdown(true)}
              className="w-full py-4 px-4 outline-none text-lg text-black placeholder-gray-400"
              placeholder="Search the web..."
              autoComplete="off"
            />
            <button 
              type="submit" 
              className="absolute right-0 top-0 bottom-0 bg-black text-white px-8 rounded-r-full flex items-center justify-center hover:bg-gray-800 transition-colors duration-200"
            >
              <ArrowRight className="h-5 w-5" />
            </button>
          </div>
        </form>

        {/* Dropdown with recent searches */}
        {showDropdown && searchHistory.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-white shadow-lg rounded-md z-20 max-h-60 overflow-y-auto custom-scrollbar">
            <div className="py-1">
              <div className="px-4 py-2 text-sm text-gray-500 border-b border-gray-100 font-medium">
                Recent searches
              </div>
              {searchHistory.map((item, index) => (
                <div
                  key={index}
                  onClick={() => handleHistoryClick(item)}
                  className="px-4 py-3 text-lg text-gray-700 hover:bg-gray-100 cursor-pointer flex items-center transition-colors duration-150"
                >
                  <Search className="h-4 w-4 text-gray-400 mr-3" />
                  <span className="truncate">{item}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Footer or additional content can go here */}
      <div className="mt-auto mb-8 text-gray-400 text-sm">
        © {new Date().getFullYear()} SearchNow
      </div>

      <style jsx>{`
        .custom-scrollbar::-webkit-scrollbar {
          width: 8px;
        }
        .custom-scrollbar::-webkit-scrollbar-track {
          background: #f1f1f1;
          border-radius: 0 4px 4px 0;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb {
          background: #c1c1c1;
          border-radius: 4px;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
          background: #a8a8a8;
        }
      `}</style>
    </div>
  );
}