package com.example.Indexer;

import java.io.*;
import java.util.*;
import java.nio.file.*;

public class StopWords
{
    private static final Set<String> STOP_WORDS = new HashSet<>();

    // Load stop words from the file when the class is initialized
    static
    {
        try
        {
            List<String> words = Files.readAllLines(Paths.get("stop_words.txt"));
            STOP_WORDS.addAll(words.stream().map(String::trim).toList());
            System.out.println(" Loaded stop words from file.");
        }
        catch (IOException e)
        {
            System.err.println(" Error loading stop words: " + e.getMessage());
        }
    }

    public static List<String> removeStopWords(List<String> words)
    {
        return words.stream().filter(word -> !STOP_WORDS.contains(word)).toList();
    }
}
