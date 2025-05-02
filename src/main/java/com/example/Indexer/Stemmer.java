package com.example.Indexer;

import org.apache.lucene.analysis.en.EnglishMinimalStemmer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;
@Component
public class Stemmer
{
    private final EnglishMinimalStemmer stemmer;
    private final Map<String, String> stemCache;
    private static final int MAX_CACHE_SIZE = 50000;

    public Stemmer()
    {
        this.stemmer = new EnglishMinimalStemmer();
        // Use LinkedHashMap with access-order to implement LRU cache
        this.stemCache =
                Collections.synchronizedMap(new LinkedHashMap<String, String>(16, 0.75f, true)
                {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, String> eldest)
                    {
                        return size() > MAX_CACHE_SIZE;
                    }
                });
    }

    public String stem(String word)
    {
        if (word == null || word.isEmpty() || word.length() <= 2)
            return word != null ? word.toLowerCase() : "";

        String lowercaseWord = word.toLowerCase();

        // Check cache first
        String cachedStem = stemCache.get(lowercaseWord);
        if (cachedStem != null)
            return cachedStem;
        try
        {
            char[] chars = lowercaseWord.toCharArray();
            int newLength = stemmer.stem(chars, chars.length);
            String result = new String(chars, 0, newLength);

            // Cache the result
            stemCache.put(lowercaseWord, result);
            return result;
        }
        catch (Exception e)
        {
            System.err.println("Stemming error for word '" + word + "': " + e.getMessage());
            return lowercaseWord;
        }
    }
}
