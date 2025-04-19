package com.example.Indexer;

public class CrawledDoc
{
    private final String url;
    private final String htmlContent;

    public CrawledDoc(String url, String htmlContent)
    {
        this.url = url;
        this.htmlContent = htmlContent;
    }

    public String getUrl()
    {
        return url;
    }

    public String getHtmlContent()
    {
        return htmlContent;
    }

}
