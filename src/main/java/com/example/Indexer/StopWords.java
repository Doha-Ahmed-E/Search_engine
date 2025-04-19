package com.example.Indexer;

import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.util.stream.Collectors;

public class StopWords
{
    private static final Set<String> STOP_WORDS = new HashSet<>();

    // Load stop words from the file when the class is initialized
    static
    {
        try
        {
            Path stopWordsPath = Paths.get("data/stop_words.txt");

            List<String> words = Files.readAllLines(stopWordsPath);
            STOP_WORDS.addAll(
                    words.stream().map(String::trim).filter(word -> !word.isBlank()).toList());
            System.out.println(" Loaded stop words from file.");

        }
        catch (IOException e)
        {
            System.err.println(" Error loading stop words: " + e.getMessage());
        }
    }


    public static List<String> removeStopWords(List<String> words)
    {
        if (words == null || words.isEmpty())
        {
            return new ArrayList<>(); // Return mutable empty list
        }

        // Return a mutable list
        return words.stream()
                .filter(word -> word != null && !word.isBlank() && !STOP_WORDS.contains(word))
                .collect(Collectors.toCollection(ArrayList::new)); // Use toCollection instead of
                                                                   // toList()
    }
}
