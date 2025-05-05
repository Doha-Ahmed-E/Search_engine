import { useState, useEffect, useRef, useLayoutEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Search, ArrowRight } from 'lucide-react';
import ResultComponent from './ResultComponent';
import { search } from '../SearchService';

const fullscreenStyle = {
  position: 'fixed',
  top: 0,
  left: 0,
  width: '100vw',
  height: '100vh',
  backgroundColor: '#0f172a',
  margin: 0,
  padding: 0,
  overflow: 'auto',
};

export default function SearchResult() {
  const location = useLocation();
  const navigate = useNavigate();
  const searchData = location.state || {};
  const [searchQuery, setSearchQuery] = useState(searchData.query || '');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTime, setSearchTime] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const resultsPerPage = 10;

  const [searchHistory, setSearchHistory] = useState(() => {
    if (typeof window !== 'undefined') {
      const storedHistory = localStorage.getItem('searchHistory');
      return storedHistory ? JSON.parse(storedHistory) : [];
    }
    return [];
  });
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);

  useEffect(() => {
    if (searchData.query) {
      handleSearchFromQuery(searchData.query);
    }
  }, [searchData.query]);

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

  const handleSearchFromQuery = async (query) => {
    if (!query.trim()) return;

    setLoading(true);
    setSearchTime(null);
    setCurrentPage(1);

    updateSearchHistory(query);

    const startTime = performance.now();

    try {
      const data = await search(query);
      console.log('Search results:', data);
      setResults(data);

      const endTime = performance.now();
      setSearchTime(Math.round(endTime - startTime));
    } catch (error) {
      console.error('Search error:', error);
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e) => {
    e.preventDefault();
    navigate('/search-result', {
      state: {
        query: searchQuery,
      },
    });
    handleSearchFromQuery(searchQuery);
    setShowDropdown(false);
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
    handleSearchFromQuery(query);
    setShowDropdown(false);
  };

  const indexOfLastResult = currentPage * resultsPerPage;
  const indexOfFirstResult = indexOfLastResult - resultsPerPage;
  const currentResults = results.slice(indexOfFirstResult, indexOfLastResult);
  const totalPages = Math.ceil(results.length / resultsPerPage);

  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage((prev) => prev + 1);
    }
  };

  const handlePrevPage = () => {
    if (currentPage > 1) {
      setCurrentPage((prev) => prev - 1);
    }
  };

  return (
    <div style={fullscreenStyle}>
      <div className="sticky top-0 z-10 bg-gray-900 shadow-md p-4">
        <div className="max-w-5xl mx-auto flex items-center">
          <h2 className="text-blue-400 text-xl font-bold mr-4">
            Search<span className="text-purple-400">Now</span>
          </h2>

          <form onSubmit={handleSearch} className="flex-1 relative" ref={dropdownRef}>
            <div className="flex items-center bg-white rounded-full shadow-lg overflow-hidden relative">
              <div className="pl-4">
                <Search className="h-5 w-5 text-gray-400" />
              </div>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onFocus={() => setShowDropdown(true)}
                className="w-full py-4 px-4 outline-none text-black text-lg"
                placeholder="Search the web..."
                disabled={loading}
              />
            </div>
            <button
              type="submit"
              className="absolute right-0 top-0 bottom-0 bg-black text-white px-8 rounded-r-full flex items-center justify-center"
              disabled={loading}
            >
              {loading ? (
                <span className="animate-pulse">...</span>
              ) : (
                <ArrowRight className="h-5 w-5" />
              )}
            </button>

            {showDropdown && searchHistory.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-2 bg-white shadow-lg rounded-md z-20 max-h-60 overflow-y-auto custom-scrollbar">
                <div className="py-1">
                  <div className="px-4 py-2 text-sm text-gray-500 border-b border-gray-100">
                    Recent searches
                  </div>
                  {searchHistory.map((item, index) => (
                    <div
                      key={index}
                      onClick={() => handleHistoryClick(item)}
                      className="px-4 py-3 text-lg text-gray-700 hover:bg-gray-100 cursor-pointer flex items-center"
                    >
                      <Search className="h-4 w-4 text-gray-400 mr-3" />
                      {item}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </form>
        </div>
      </div>

      <div className="max-w-5xl mx-auto p-6 pt-4">
        <p className="text-gray-300 mb-2">
          {loading ? 'Searching...' : `Results for: ${searchQuery || 'All'}`}
        </p>

        {searchTime !== null && !loading && (
          <p className="text-gray-400 text-sm mb-6">
            Search completed in {searchTime} ms
          </p>
        )}

        <ResultComponent result={currentResults} />

        {results.length > 0 && !loading && (
          <div className="flex items-center justify-between mt-6">
            <button
              onClick={handlePrevPage}
              disabled={currentPage === 1}
              className={`px-4 py-2 rounded ${
                currentPage === 1
                  ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700'
              }`}
            >
              Previous
            </button>

            <span className="text-gray-300">
              Page {currentPage} of {totalPages}
            </span>

            <button
              onClick={handleNextPage}
              disabled={currentPage === totalPages}
              className={`px-4 py-2 rounded ${
                currentPage === totalPages
                  ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700'
              }`}
            >
              Next
            </button>
          </div>
        )}
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