export default function ResultComponent({ result }) {
  if (!Array.isArray(result) || result.length === 0) {
    return <p className="text-gray-400">No results found.</p>;
  }

  // Helper function to replace <strong> tags with styled spans
  const formatSnippet = (snippet) => {
    if (!snippet) return '';

    return snippet.replace(/<strong>(.*?)<\/strong>/g, (match, p1) => {
      return `<span class="text-yellow-400 font-bold">${p1}</span>`;
    });
  };

  return (
    <div className="space-y-6">
      {result.map((item) => (
        <div key={item.url} className="bg-gray-800 rounded-lg p-4 hover:bg-gray-700 cursor-pointer">
          <h2 className="text-blue-400 text-xl font-medium mb-1">{item.title}</h2>
          <a
            href={item.url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-green-400 text-sm mb-2 hover:underline block"
          >
            {item.url}
          </a>
          <p
            className="text-gray-300"
            dangerouslySetInnerHTML={{
              __html: formatSnippet(
                item.snippet?.endsWith('.') ? item.snippet : `${item.snippet}.`
              ),
            }}
          />
        </div>
      ))}
    </div>
  );
}