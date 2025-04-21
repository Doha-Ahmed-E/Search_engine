package com.example.QueryProcessor;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class QDocument
{
    @JsonProperty("term_stats")
    private Map<String, TermStats> termStats;

    @JsonProperty("metadata")
    private Metadata metadata;

    public Map<String, TermStats> getTermStats()
    {
        return termStats;
    }

    public void setTermStats(Map<String, TermStats> termStats)
    {
        this.termStats = termStats;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public void setMetadata(Metadata metadata)
    {
        this.metadata = metadata;
    }
}
