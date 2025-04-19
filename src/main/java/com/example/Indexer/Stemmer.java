package com.example.Indexer;

import org.tartarus.snowball.ext.PorterStemmer;
import java.util.List;
import java.util.ArrayList;

public class Stemmer
{
    private static final boolean DEBUG = false;
    private static final ThreadLocal<PorterStemmer> stemmerThreadLocal =
            ThreadLocal.withInitial(PorterStemmer::new);

    public static String stem(String word)
    {
        if (word == null || word.isBlank())
        {
            System.err.println("Skipping empty or null word during stemming.");
            return word;
        }

        PorterStemmer stemmerInstance = stemmerThreadLocal.get();
        stemmerInstance.setCurrent(word);
        stemmerInstance.stem();
        String stemmedWord = stemmerInstance.getCurrent();

        if (DEBUG)
        {
            System.out.println("🔍 Stemming: " + word + " → " + stemmedWord);
        }
        return stemmedWord;
    }

    // Add a batch processing method for efficiency
    public static List<String> stemAll(List<String> words)
    {
        List<String> stemmedWords = new ArrayList<>(words.size());
        for (String word : words)
        {
            stemmedWords.add(stem(word));
        }
        return stemmedWords;
    }
}
