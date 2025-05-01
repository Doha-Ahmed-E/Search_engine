package com.example.Indexer;

public class CrawledDoc
{
    private String url;
    private String title;
    private String htmlContent;
    private String mongoId; // Add this field
    private String docId; // Add this field

    public CrawledDoc(String url, String title, String htmlContent)
    {
        this.url = url;
        this.htmlContent = htmlContent;
        this.title = title;
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
}
