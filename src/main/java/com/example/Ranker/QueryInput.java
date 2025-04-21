package com.example.Ranker;

import com.example.QueryProcessor.QDocument;
import com.example.QueryProcessor.GlobalStats;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

// Only leave the QueryInput class in this file
public class QueryInput
{
    @JsonProperty("query_terms")
    private List<String> queryTerms;

    @JsonProperty("candidate_documents")
    private Map<String, QDocument> candidateDocuments;

    @JsonProperty("global_stats")
    private GlobalStats globalStats;

    // Getters and Setters
    public List<String> getQueryTerms()
    {
        return queryTerms;
    }

    public void setQueryTerms(List<String> queryTerms)
    {
        this.queryTerms = queryTerms;
    }

    public Map<String, QDocument> getCandidateDocuments()
    {
        return candidateDocuments;
    }

    public void setCandidateDocuments(Map<String, QDocument> candidateDocuments)
    {
        this.candidateDocuments = candidateDocuments;
    }

    public GlobalStats getGlobalStats()
    {
        return globalStats;
    }

    public void setGlobalStats(GlobalStats globalStats)
    {
        this.globalStats = globalStats;
    }
}
