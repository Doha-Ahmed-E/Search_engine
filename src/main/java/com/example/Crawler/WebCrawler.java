package com.example.Crawler;

import com.mongodb.client.MongoCollection;
import com.example.DatabaseConnection;
import com.mongodb.client.model.Filters;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import java.util.*;
import java.util.concurrent.*;

public class WebCrawler
{
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private final PagePriority priorityQueue = new PagePriority();
    private final int MAX_PAGES = 6000;
    private final int THREAD_COUNT = 5;
    private final MongoCollection<org.bson.Document> collection =
            DatabaseConnection.getDatabase().getCollection("pages");


    public WebCrawler(String seedUrl)
    {
        priorityQueue.addPage(seedUrl, 100);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// start the crawling process by creating a thread pool and processing pages from the priority
    //////////////////////////////////////////////////////////////////////////////////////////////////////// queue.
    /// It uses a priority queue to manage the pages based on their link scores.
    /// The method also handles the termination of the thread pool after the crawling is complete.
    /// It ensures that all threads finish their tasks before shutting down.

    public void startCrawling()
    {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        try
        {
            while (!priorityQueue.isEmpty() && visitedUrls.size() < MAX_PAGES)
            {
                PagePriority.WebPage page = priorityQueue.getNextPage();
                if (page != null && visitedUrls.add(page.url))
                    executor.execute(() -> crawlPage(page.url));

            }
        } finally
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination(10, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                System.err.println("Thread pool interrupted: " + e.getMessage());
            }
        }

    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// This method is responsible for crawling a single page.
    /// It checks if the page is allowed to be crawled based on the robots.txt file,
    /// fetches the page content, extracts links, and saves the page to the database.
    /// It also checks for duplicate content using a hash of the page content.
    /// If the page is a duplicate, it skips saving it to the database.
    /// The method also extracts the title and outgoing links from the crawled page.
    /// It uses the Jsoup library to connect to the URL and parse the HTML content.
    /// The method generates a hash of the page content using the PageHasher class
    /// to check for duplicates before saving the page to the MongoDB database.

    private void crawlPage(String url)
    {
        try
        {
            if (!RobotsChecker.isAllowed(url))
                return;

            Document doc = Jsoup.connect(url).get();
            System.out.println("Crawled: " + url);

            String content = doc.body().text();
            String title = doc.title();
            String contentHash = PageHasher.generateHash(content);

            if (isDuplicate(contentHash))
                return;

            Elements links = doc.select("a[href]");
            List<String> outgoingLinks = new ArrayList<>();

            for (org.jsoup.nodes.Element link : links)
            {
                String nextUrl = link.absUrl("href");
                if (!visitedUrls.contains(nextUrl))
                {
                    priorityQueue.addPage(nextUrl, links.size());
                    outgoingLinks.add(nextUrl);
                }
            }

            savePage(url, title, content, contentHash, outgoingLinks);
        }
        catch (Exception e)
        {
            System.err.println("Error crawling: " + url + " | " + e.getMessage());
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// This method checks if a page with the same content hash already exists in the database.
    /// It uses the MongoDB collection to query for documents with the same content hash.
    /// If a document with the same hash is found, it returns true, indicating a duplicate.
    /// The method uses the Filters class from the MongoDB driver to create a query


    private boolean isDuplicate(String hash)
    {
        return collection.find(Filters.eq("contentHash", hash)).first() != null;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// save the crawled page to the MongoDB database.

    private void savePage(String url, String title, String content, String hash,
            List<String> outgoingLinks)
    {
        org.bson.Document pageDoc =
                new org.bson.Document("url", url).append("title", title).append("content", content)
                        .append("contentHash", hash).append("outgoingLinks", outgoingLinks);

        collection.insertOne(pageDoc);
        System.out.println("✅ Stored in MongoDB: " + url);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////
    /// main method to initialize the WebCrawler with a seed URL
    public static void main(String[] args)
    {
        WebCrawler crawler = new WebCrawler("https://example.com");
        crawler.startCrawling();
    }
}
