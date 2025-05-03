package com.example.QueryProcessor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TermStats
{
    @JsonProperty("tf")
    private double tf;

    @JsonProperty("in_title")
    private boolean inTitle;

    @JsonProperty("importance_score")
    private double importanceScore;
    public TermStats() {}

    public TermStats(boolean inTitle)
    {
        this.inTitle = inTitle;
    }
    public TermStats(boolean inTitle,double tf,double importanceScore)
    {
        this.inTitle = inTitle;
        this.tf = tf;
        this.importanceScore = importanceScore;
    }


    public double getTf()
    {
        return tf;
    }

    public void setTf(double tf)
    {
        this.tf = tf;
    }

    public boolean isInTitle()
    {
        return inTitle;
    }

    public void setInTitle(boolean inTitle)
    {
        this.inTitle = inTitle;
    }

    public void setImportanceScore(double importanceScore)
    {
        this.importanceScore = importanceScore;
    }

    public double getImportanceScore()
    {
        return importanceScore;
    }
}
