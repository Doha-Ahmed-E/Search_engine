package com.example.Indexer;

import org.tartarus.snowball.ext.PorterStemmer;

public class Stemmer
{
    private final PorterStemmer stemmer;

    public Stemmer()
    {
        this.stemmer = new PorterStemmer();
    }

    public String stem(String word)
    {
        if (word == null || word.isBlank())
            return null;

        stemmer.setCurrent(word);
        if (stemmer.stem())
        {
            // If stemming is successful, return the stemmed word
            return stemmer.getCurrent();
        }
        // If stemming fails, return the original word

        return word;
    }
}
