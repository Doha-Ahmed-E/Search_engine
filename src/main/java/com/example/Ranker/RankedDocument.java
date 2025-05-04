package com.example.Ranker;

public class RankedDocument {
    private final String docId;
    private final String URL;
    private final String snippet;
    private final String title;
    private final double score;
    private final double relevance;
    private final double popularity;

    // Constructor and getters
    public RankedDocument(String docId, double score, double relevance, double popularity,String URL,String snippet,String title) {
        this.docId = docId;
        this.score = score;
        this.relevance = relevance;
        this.popularity = popularity;
        this.URL = URL;
        this.snippet = snippet; 
        this.title = title; 
    }

    public String getSnippet() {
        return snippet; 
    }

    public String getTitle() {
        return title; 
    }

    public String getURL()
    {
        return URL;
    }
    public String getDocId()
    {
        return docId;
    }

    public double getScore()
    {
        return score;
    }

    public double getRelevance()
    {
        return relevance;
    }

    public double getPopularity()
    {
        return popularity;
    }
}