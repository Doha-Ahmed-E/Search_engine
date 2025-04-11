package com.example.Crawler;

import java.util.PriorityQueue;

public class PagePriority
{
    private PriorityQueue<WebPage> queue = new PriorityQueue<>();

    public void addPage(String url, int linkScore)
    {
        queue.add(new WebPage(url, linkScore));
    }

    public WebPage getNextPage()
    {
        return queue.poll(); // Fetch highest priority page
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class WebPage implements Comparable<WebPage>
    {
        String url;
        int linkScore;

        public WebPage(String url, int linkScore)
        {
            this.url = url;
            this.linkScore = linkScore;
        }

        @Override
        public int compareTo(WebPage other)
        {
            return Integer.compare(other.linkScore, this.linkScore);
        }
    }
}
