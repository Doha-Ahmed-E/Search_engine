
export default function ResultComponent({Result}){
    return (
        <div className="space-y-6">
        {Result.map((item) => (
          <div key={item} className="bg-gray-800 rounded-lg p-4 hover:bg-gray-700 cursor-pointer">
            <h2 className="text-blue-400 text-xl font-medium mb-1">{item.title}</h2>
            <p className="text-green-400 text-sm mb-2">{item.url}</p>
            <p className="text-gray-300">{item.content}.</p>
          </div>
        ))}
      </div>
    );
}