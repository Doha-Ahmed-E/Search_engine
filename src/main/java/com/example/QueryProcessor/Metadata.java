package com.example.QueryProcessor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Metadata
{
    @JsonProperty("popularity")
    private double popularity;

    @JsonProperty("length")
    private int length;

    @JsonProperty("publish_date")
    private String publishDate;

    @JsonProperty("URL")
    private String url;

    public double getPopularity()
    {
        return popularity;
    }

    public void setPopularity(double popularity)
    {
        this.popularity = popularity;
    }

    public int getLength()
    {
        return length;
    }

    public void setLength(int length)
    {
        this.length = length;
    }

    public String getPublishDate()
    {
        return publishDate;
    }

    public void setPublishDate(String publishDate)
    {
        this.publishDate = publishDate;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }
}
