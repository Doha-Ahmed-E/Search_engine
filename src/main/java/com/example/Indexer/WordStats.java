package com.example.Indexer;

import java.util.ArrayList;
import java.util.List;

public class WordStats
{
    private int frequency = 0;
    private double importanceScore = 0.0;
    private final List<Integer> positions = new ArrayList<>();

    public void addOccurrence(double weight, int position)
    {
        frequency++;
        importanceScore += weight;
        positions.add(position);
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
}
