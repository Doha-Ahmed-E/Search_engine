package com.example.Indexer;

public class CrawledDoc
{
    private String url;
    private String title;
    private String htmlContent;
    private String mongoId; // Add this field
    private String docId; // Add this field
    private double popularity;

    public CrawledDoc(String url, String title, double popularity, String htmlContent)
    {
        this.url = url;
        this.htmlContent = htmlContent;
        this.title = title;
        this.popularity = popularity;
    }

    public String getUrl()
    {
        return url;
    }

    public String getHtmlContent()
    {
        return htmlContent;
    }

    public String getTitle()
    {
        return title;
    }

    public String getMongoId()
    {
        return mongoId;
    }

    public void setMongoId(String mongoId)
    {
        this.mongoId = mongoId;
    }

    public String getDocId()
    {
        return docId;
    }

    public void setDocId(String docId)
    {
        this.docId = docId;
    }

    public double getPopularity()
    {
        return popularity;
    }

    public void setPopularity(double popularity)
    {
        this.popularity = popularity;
    }
}
