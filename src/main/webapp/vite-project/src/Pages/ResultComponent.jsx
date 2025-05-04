export default function ResultComponent({ result }) {
  if (!Array.isArray(result) || result.length === 0) {
    return <p className="text-gray-400">No results found.</p>;
  }

  return (
    <div className="space-y-6">
      {result.map((item) => (
        <div key={item.url} className="bg-gray-800 rounded-lg p-4 hover:bg-gray-700 cursor-pointer">
          <h2 className="text-blue-400 text-xl font-medium mb-1">{item.title}</h2>
          <p className="text-green-400 text-sm mb-2">{item.url}</p>
          <p className="text-gray-300">
            {item.snippet?.endsWith('.') ? item.snippet : `${item.snippet}.`}
          </p>
        </div>
      ))}
    </div>
  );
}