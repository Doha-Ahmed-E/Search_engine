import { useState } from 'react';
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
  overflow: 'hidden'
};

export default function SearchPage() {
  const [searchQuery, setSearchQuery] = useState('');
  const navigate = useNavigate();
  
  
  const handleSearch = (e) => {
    e.preventDefault();
    navigate('/search-result', { 
      state: { 
        query: searchQuery ,
      } 
    });
  };

  return (
    <div style={fullscreenStyle} className="flex flex-col items-center justify-center">
      <div className="mb-12">
        <h1 className="text-6xl font-bold text-blue-400">
          Search<span className="text-purple-400">Now</span>
        </h1>
      </div>

      <div className="w-11/12 max-w-2xl">
        <form onSubmit={handleSearch} className="relative">
          <div className="flex items-center bg-white rounded-full shadow-xl overflow-hidden">
            <div className="pl-4">
              <Search className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full py-4 px-4 outline-none text-lg text-black"
              placeholder="Search the web..."
            />
          </div>
          <button 
            type="submit" 
            className="absolute right-0 top-0 bottom-0 bg-black text-white px-8 rounded-r-full flex items-center justify-center"
            onClick={handleSearch} // Added explicit onClick handler
          >
            <ArrowRight className="h-5 w-5" />
          </button>
        </form>
      </div>
    </div>
  );
}