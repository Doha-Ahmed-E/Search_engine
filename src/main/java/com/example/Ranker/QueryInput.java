package com.example.Ranker;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class QueryInput {
    @JsonProperty("query_terms")
    private List<String> queryTerms;
    
    @JsonProperty("candidate_documents")
    private Map<String, Document> candidateDocuments;
    
    @JsonProperty("global_stats")
    private GlobalStats globalStats;

    // Getters and Setters
    public List<String> getQueryTerms() { return queryTerms; }
    public void setQueryTerms(List<String> queryTerms) { this.queryTerms = queryTerms; }
    
    public Map<String, Document> getCandidateDocuments() { return candidateDocuments; }
    public void setCandidateDocuments(Map<String, Document> candidateDocuments) { 
        this.candidateDocuments = candidateDocuments; 
    }
    
    public GlobalStats getGlobalStats() { return globalStats; }
    public void setGlobalStats(GlobalStats globalStats) { this.globalStats = globalStats; }
}

class Document {
    @JsonProperty("term_stats")
    private Map<String, TermStats> termStats;
    private Metadata metadata;

    public Map<String, TermStats> getTermStats() { return termStats; }
    public void setTermStats(Map<String, TermStats> termStats) { this.termStats = termStats; }
    
    public Metadata getMetadata() { return metadata; }
    public void setMetadata(Metadata metadata) { this.metadata = metadata; }
}

class TermStats {
    private double tf;
    @JsonProperty("in_title")
    private boolean inTitle;

    public double getTf() { return tf; }
    public void setTf(double tf) { this.tf = tf; }
    
    public boolean isInTitle() { return inTitle; }
    public void setInTitle(boolean inTitle) { this.inTitle = inTitle; }
}

class Metadata {
    private double popularity;
    private int length;
    @JsonProperty("publish_date")
    private String publishDate;

    public double getPopularity() { return popularity; }
    public void setPopularity(double popularity) { this.popularity = popularity; }
    
    public int getLength() { return length; }
    public void setLength(int length) { this.length = length; }
    
    public String getPublishDate() { return publishDate; }
    public void setPublishDate(String publishDate) { this.publishDate = publishDate; }
}

class GlobalStats {
    @JsonProperty("total_docs")
    private int totalDocs;
    @JsonProperty("docs_containing_term")
    private Map<String, Integer> docsContainingTerm;

    public int getTotalDocs() { return totalDocs; }
    public void setTotalDocs(int totalDocs) { this.totalDocs = totalDocs; }
    
    public Map<String, Integer> getDocsContainingTerm() { return docsContainingTerm; }
    public void setDocsContainingTerm(Map<String, Integer> docsContainingTerm) { 
        this.docsContainingTerm = docsContainingTerm; 
    }
}