import { BrowserRouter, Routes, Route } from 'react-router-dom';
import SearchPage from './Pages/SearchPage';
import SearchResult from './Pages/SearchResult';
import './App.css'
function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<SearchPage />} />
        <Route path="/search-result" element={<SearchResult />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;