package com.example.Indexer;

import org.tartarus.snowball.ext.PorterStemmer;

public class Stemmer
{
    private static final PorterStemmer stemmer = new PorterStemmer();

    public static String stem(String word)
    {
        stemmer.setCurrent(word);
        stemmer.stem();
        return stemmer.getCurrent();
    }
}

