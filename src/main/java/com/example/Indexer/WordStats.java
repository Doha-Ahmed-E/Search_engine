package com.example.Indexer;

public class WordStats
{
    private int frequency = 0;
    private double importanceScore = 0.0;

    public void addOccurrence(double weight)
    {
        frequency++;
        importanceScore += weight;
    }

    public int getFrequency()
    {
        return frequency;
    }

    public double getImportanceScore()
    {
        return importanceScore;
    }
}
