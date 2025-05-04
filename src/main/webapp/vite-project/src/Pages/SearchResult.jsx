import { useState, useEffect } from 'react';
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
  overflow: 'auto' 
};

export default function SearchResult() {
  const location = useLocation();
  const navigate = useNavigate();
  const searchData = location.state || {};
  const [searchQuery, setSearchQuery] = useState(searchData.query || '');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  
  // Load results when component mounts or query changes
  useEffect(() => {
    if (searchData.query) {
      handleSearchFromQuery(searchData.query);
    }
  }, [searchData.query]);

  const handleSearchFromQuery = async (query) => {
    if (!query.trim()) return;
    
    setLoading(true);
    try {
      const data = await search(query);
      console.log('Search results:', data);
      setResults(data);
    } catch (error) {
      console.error('Search error:', error);
      setResults(null);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e) => {
    e.preventDefault();
    navigate('/search-result', { 
      state: { 
        query: searchQuery,
      } 
    });
    handleSearchFromQuery(searchQuery);
  };


  return (
    <div style={fullscreenStyle}>
      <div className="sticky top-0 z-10 bg-gray-900 shadow-md p-4">
        <div className="max-w-5xl mx-auto flex items-center">
          <h2 className="text-blue-400 text-xl font-bold mr-4">
            Search<span className="text-purple-400">Now</span>
          </h2>
          
          <form onSubmit={handleSearch} className="flex-1 relative">
            <div className="flex items-center bg-white rounded-full shadow-lg overflow-hidden">
              <div className="pl-3">
                <Search className="h-4 w-4 text-gray-400" />
              </div>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full py-2 px-3 outline-none text-black text-sm"
                placeholder="Search the web..."
                disabled={loading}
              />
            </div>
            <button 
              type="submit" 
              className="absolute right-0 top-0 bottom-0 bg-black text-white px-4 rounded-r-full flex items-center justify-center"
              disabled={loading}
            >
              {loading ? (
                <span className="animate-pulse">...</span>
              ) : (
                <ArrowRight className="h-4 w-4" />
              )}
            </button>
          </form>
        </div>
      </div>

      <div className="max-w-5xl mx-auto p-6 pt-4">
        <p className="text-gray-300 mb-6">
          {loading ? (
            'Searching...'
          ) : (
            `Results for: ${searchData.query || 'All'}`
          )}
        </p>
        
        <ResultComponent 
          result={results || []} 
        />
      </div>
    </div>
  );
}