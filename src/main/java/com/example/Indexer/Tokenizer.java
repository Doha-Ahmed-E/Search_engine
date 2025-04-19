package com.example.Indexer;

import opennlp.tools.tokenize.SimpleTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tokenizer
{
    private final SimpleTokenizer tokenizer;

    public Tokenizer()
    {
        this.tokenizer = SimpleTokenizer.INSTANCE;
    }

    public List<String> tokenize(String text)
    {

        if (text == null || text.isBlank())
            return new ArrayList<>(); // Return mutable empty list

        List<String> tokens = new ArrayList<>(Arrays.asList(tokenizer.tokenize(text)));
        tokens.removeIf(word -> !word.matches("[a-zA-Z]+"));

        return tokens;
    }
}
