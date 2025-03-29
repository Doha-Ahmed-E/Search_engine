package com.example;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class WebCrawler
{
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet(); // Avoid duplicates
    private final Queue<String> urlQueue = new ConcurrentLinkedQueue<>(); // URL queue
    private final int MAX_PAGES = 6000; // Limit number of crawled pages
    private final int THREAD_COUNT = 5; // Adjust based on requirements

    public WebCrawler(String seedUrl)
    {
        urlQueue.add(seedUrl);
    }

    public void startCrawling()
    {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        while (!urlQueue.isEmpty() && visitedUrls.size() < MAX_PAGES)
        {
            String url = urlQueue.poll();
            if (url != null && visitedUrls.add(url))
            { // Avoid revisits
                executor.execute(() -> crawlPage(url));
            }
        }
        executor.shutdown();
        try
        {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void crawlPage(String url)
    {
        try
        {
            if (!isAllowedByRobotsTxt(url))
                return; // Respect robots.txt

            Document doc = Jsoup.connect(url).get();
            System.out.println("Crawled: " + url);

            Elements links = doc.select("a[href]");
            for (var link : links)
            {
                String nextUrl = link.absUrl("href");
                if (!visitedUrls.contains(nextUrl) && isValidUrl(nextUrl))
                {
                    urlQueue.add(nextUrl);
                }
            }
            savePage(url, doc.html()); // Save the HTML content
        }
        catch (Exception e)
        {
            System.err.println("Error crawling: " + url);
        }
    }

    private boolean isAllowedByRobotsTxt(String url)
    {
        try
        {
            URL baseUrl = new URL(url);
            String robotsTxtUrl = baseUrl.getProtocol() + "://" + baseUrl.getHost() + "/robots.txt";
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(new URL(robotsTxtUrl).openStream()));
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.contains("Disallow: /"))
                {
                    return false; // If disallowed, don't crawl
                }
            }
        }
        catch (IOException ignored)
        {
        }
        return true;
    }

    private boolean isValidUrl(String url)
    {
        return url.startsWith("http") && (url.endsWith(".html") || url.endsWith("/"));
    }

    private void savePage(String url, String content)
    {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("crawled_pages.txt", true)))
        {
            writer.write("URL: " + url + "\n" + content + "\n\n");
        }
        catch (IOException e)
        {
            System.err.println("Failed to save: " + url);
        }
    }

    public static void main(String[] args)
    {
        WebCrawler crawler = new WebCrawler("https://example.com");
        crawler.startCrawling();
    }
}
