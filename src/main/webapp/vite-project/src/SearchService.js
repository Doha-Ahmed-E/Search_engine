export const search = async (query, limit = 10) => {
  try {
    // Use full URL for debugging
    const url = `http://localhost:8080/api/search?q=${encodeURIComponent(query)}&limit=${limit}`;
    console.log('Request URL:', url); // Debugging
    
    const response = await fetch(url, {
      headers: { 'Accept': 'application/json' }
    });

    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error('Search failed:', error.message);
    throw error;
  }
};