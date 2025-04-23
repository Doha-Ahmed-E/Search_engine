package com.example.Indexer;

public class CrawledDoc
{
    private final String url;
    private final String htmlContent;
    private final String title;

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

}
