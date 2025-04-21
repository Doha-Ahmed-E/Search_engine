package com.example.QueryProcessor;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class GlobalStats
{
    @JsonProperty("total_docs")
    private int totalDocs;

    @JsonProperty("docs_containing_term")
    private Map<String, Integer> docsContainingTerm;

    @JsonProperty("term_idf")
    private Map<String, Double> termIDF;

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

    public Map<String, Double> getTermIDF()
    {
        return termIDF;
    }

    public void setTermIDF(Map<String, Double> termIDF)
    {
        this.termIDF = termIDF;
    }
}
