package com.example.Indexer;

import org.tartarus.snowball.ext.PorterStemmer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Stemmer
{
    private final PorterStemmer stemmer;
    private final Map<String, String> stemCache;
    private static final int MAX_CACHE_SIZE = 50000;

    public Stemmer()
    {
        this.stemmer = new PorterStemmer();
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
        // Return null for single characters, empty strings, or pure numbers
        if (word == null || word.isEmpty() || word.length() <= 1 || isNumeric(word))
            return null;

        // Don't stem but keep short words (2-3 chars)
        if (word.length() <= 3)
            return word.toLowerCase();

        String lowercaseWord = word.toLowerCase();

        // Check cache first
        String cachedStem = stemCache.get(lowercaseWord);
        if (cachedStem != null)
            return cachedStem;

        try
        {
            // Use Snowball PorterStemmer
            stemmer.setCurrent(lowercaseWord);
            stemmer.stem();
            String result = stemmer.getCurrent();

            // Cache the result
            stemCache.put(lowercaseWord, result);
            return result;
        }
        catch (Exception e)
        {
            return lowercaseWord;
        }
    }

    // Helper method to check if a string is numeric
    private boolean isNumeric(String str)
    {
        if (str == null || str.isEmpty())
        {
            return false;
        }

        // Check if the string contains only digits
        for (char c : str.toCharArray())
        {
            if (!Character.isDigit(c))
            {
                return false;
            }
        }

        return true;
    }
}
