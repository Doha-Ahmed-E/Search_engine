package com.example.Indexer;

import opennlp.tools.tokenize.SimpleTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tokenizer
{
    private static final SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
    private static final boolean DEBUG = false; // Toggle for debugging output

    public static List<String> tokenize(String text)
    {
        if (text == null || text.isBlank())
        {
            System.err.println(" Warning: Attempted to tokenize a null or empty string.");
            return new ArrayList<>(); // Return mutable empty list
        }

        // Using ArrayList to ensure the result is mutable
        List<String> tokens = new ArrayList<>(Arrays.asList(tokenizer.tokenize(text)));

        if (DEBUG)
        {
            System.out.println("🔍 Tokenized: " + tokens);
        }
        return tokens;
    }
}
