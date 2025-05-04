package com.example.Indexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordStats
{
    private int frequency = 0;
    private double importanceScore = 0.0;
    private final List<Integer> positions = new ArrayList<>();
    private final Map<Integer, String> contextSnippets = new HashMap<>();

    public void addOccurrence(double weight, int position, String context)
    {
        frequency++;
        importanceScore += weight;
        positions.add(position);
        contextSnippets.put(position, context);
    }

    public int getFrequency()
    {
        return frequency;
    }

    public double getImportanceScore()
    {
        return importanceScore;
    }

    public List<Integer> getPositions()
    {
        return positions;
    }

    public Map<Integer, String> getContextSnippets()
    {
        return contextSnippets;
    }
}
