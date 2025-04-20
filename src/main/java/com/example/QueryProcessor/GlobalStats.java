package com.example.QueryProcessor;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class GlobalStats
{
    @JsonProperty("total_docs")
    private int totalDocs;

    @JsonProperty("docs_containing_term")
    private Map<String, Integer> docsContainingTerm;

    public int getTotalDocs()
    {
        return totalDocs;
    }

    public void setTotalDocs(int totalDocs)
    {
        this.totalDocs = totalDocs;
    }

    public Map<String, Integer> getDocsContainingTerm()
    {
        return docsContainingTerm;
    }

    public void setDocsContainingTerm(Map<String, Integer> docsContainingTerm)
    {
        this.docsContainingTerm = docsContainingTerm;
    }
}
