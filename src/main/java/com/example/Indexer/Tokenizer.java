package com.example.Indexer;

import opennlp.tools.tokenize.SimpleTokenizer;
import java.util.Arrays;
import java.util.List;

public class Tokenizer
{
    private static final SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;

    public static List<String> tokenize(String text)
    {
        return Arrays.asList(tokenizer.tokenize(text));
    }
}

