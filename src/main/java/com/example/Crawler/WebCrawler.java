package com.example.Crawler;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.Elements;

import com.example.DatabaseConnection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class WebCrawler
{
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<String> urlQueue = new LinkedBlockingQueue<>();
    private final int MAX_PAGES;
    private final int THREAD_COUNT;
    private final String DEFAULT_SEEDS_FILE;
    private final MongoCollection<org.bson.Document> collection;

    // Counters to track crawler statistics
    private final AtomicInteger savedCount = new AtomicInteger(0); // Pages successfully saved to DB
    private final AtomicInteger attemptedCount = new AtomicInteger(0); // Pages attempted
    private final AtomicInteger skippedCount = new AtomicInteger(0); // Pages skipped
    private final DomainRateLimiter rateLimiter = new DomainRateLimiter();

    private volatile boolean isRunning = true; // Flag to control crawler operation
    private final Object saveLock = new Object(); // Lock to synchronize saving operations


    public WebCrawler(String seedsFile, int maxPages, int threadCount)
    {
        this.MAX_PAGES = maxPages;
        this.THREAD_COUNT = threadCount;
        DEFAULT_SEEDS_FILE = seedsFile;
        this.collection = DatabaseConnection.getDatabase().getCollection("clean_pages");

        // load seed URLs from file and add them to the queue
        List<String> seedUrls = loadSeedsFromFile(DEFAULT_SEEDS_FILE);
        for (String seed : seedUrls)
            addSeedUrl(seed);
    }

    // start the crawler
    public void startCrawling()
    {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT); // Count down when threads finish

        // Create and start worker threads
        for (int i = 0; i < THREAD_COUNT; i++)
        {
            executor.submit(() -> {
                try
                {
                    crawlWorker();
                }
                catch (Exception e)
                {
                    System.err.println("Worker thread error: " + e.getMessage());
                    e.printStackTrace();
                } finally
                {
                    latch.countDown(); // Count down when thread finishes
                }
            });
        }
        try
        {
            // Wait for all threads to finish, but with a timeout to prevent hanging
            boolean completed = latch.await(30, TimeUnit.MINUTES);
            if (!completed)
                System.out.println("Crawler timed out after 30 minutes");
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } finally
        {
            // Stop the crawler and shut down thread pool
            stopCrawling();
            executor.shutdownNow();
            try
            {
                // Wait briefly for threads to terminate
                if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                    System.out.println("Executor did not terminate cleanly");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void crawlWorker()
    {
        while (isRunning)
        {
            // Check max pages with centralized method
            if (isMaxPagesReached())
            {
                System.out.println(
                        Thread.currentThread().getName() + ": Max pages reached, stopping.");
                break;
            }

            String url = null;
            try
            {
                url = urlQueue.poll(5, TimeUnit.SECONDS); // Get next URL from queue with timeout
                if (url == null)
                {
                    handleEmptyQueue();
                    continue;
                }

                // Normalize and check if already visited
                String normalizedUrl = UrlNormalizer.normalize(url);
                if (normalizedUrl == null || !visitedUrls.add(normalizedUrl))
                    continue;

                // Process the page
                attemptedCount.incrementAndGet();
                processPage(normalizedUrl);

                // Log progress
                logProgress(); // Log the progress of the crawler
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e)
            {
                System.err.println("Error processing URL: " + url + " - " + e.getMessage());
            }
        }
    }

    // Handle empty queue situation
    private boolean handleEmptyQueue() throws InterruptedException
    {
        if (urlQueue.isEmpty() && !isMaxPagesReached())
        {
            System.out.println(Thread.currentThread().getName()
                    + ": Queue empty, waiting 5s for new URLs... Saved: " + savedCount.get());
            Thread.sleep(5000);

            if (urlQueue.isEmpty())
            {
                System.out.println(Thread.currentThread().getName()
                        + ": Queue still empty after waiting, thread exiting.");
                return false; // Signal to exit loop
            }
        }
        return true; // Continue processing
    }

    // fetch content, check if it's valid/unique, extract links, and save to database
    private void processPage(String url)
    {
        try
        {
            // Check if the URL is allowed by robots.txt
            if (!RobotsChecker.isAllowed(url))
            {
                System.out.println("Disallowed by robots.txt: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            // Apply rate limiting for the domain
            rateLimiter.waitForDomain(url);
            if (Thread.currentThread().isInterrupted())
            {
                System.out.println("Interrupted during rate limiter wait: " + url);
                return;
            }

            // Fetch the page content with retries
            Document doc = fetchWithRetries(url);
            if (doc == null)
            {
                System.out.println("Failed to fetch page: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            // Check if the content is HTML
            if (!isHtmlContent(doc))
            {
                System.out.println("Not HTML content: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            // Extract content and generate hash for duplicate detection
            String title = doc.title();
            String content = doc.body().text();
            String contentHash = PageHasher.generateHash(content);

            // Check if we already have this content in the database
            if (isDuplicate(contentHash))
            {
                System.out.println("Duplicate content: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            // Process links for outgoing links and queue new URLs
            List<String> outgoingLinks = processLinks(doc);

            // Save the page to the database
            savePage(url, title, content, contentHash, outgoingLinks);
        }
        catch (Exception e)
        {
            System.err.println("Failed to process " + url + ": " + e.getMessage());
            skippedCount.incrementAndGet();
        }
    }

    // Check if the content type is HTML
    private boolean isHtmlContent(Document doc)
    {
        String contentType = doc.connection().response().contentType();
        return contentType != null && contentType.toLowerCase().startsWith("text/html");
    }

    // Fetch the page with retries in case of failure
    private Document fetchWithRetries(String url)
    {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++)
        {
            if (Thread.currentThread().isInterrupted())
            {
                System.out.println("Interrupted during fetch: " + url);
                return null;
            }
            try
            {
                // Use Jsoup to fetch the page
                return Jsoup.connect(url).userAgent(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36")
                        .timeout(10000) // 10 second timeout
                        .followRedirects(true).get();
            }
            catch (IOException e)
            {
                System.err.println(
                        "Attempt " + (i + 1) + " failed for " + url + ": " + e.getMessage());
                if (i == maxRetries - 1)
                    return null; // Give up after max retries
                try
                {
                    Thread.sleep(2000); // Wait 2 seconds between retries
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    // Process links and return the list of outgoing links
    private List<String> processLinks(Document doc)
    {
        Elements links = doc.select("a[href]");
        List<String> allOutgoingLinks = new ArrayList<>();
        int added = 0;

        for (Element link : links)
        {
            String absUrl = link.absUrl("href");

            // Add to outgoing links list (for page metadata)
            if (!absUrl.isEmpty())
                allOutgoingLinks.add(absUrl);

            // Check if we should add to crawl queue
            if (added < 50)
            {
                String normalized = UrlNormalizer.normalize(absUrl);
                if (normalized != null && !normalized.isEmpty() && normalized.startsWith("http")
                        && !visitedUrls.contains(normalized) && !urlQueue.contains(normalized))
                {
                    urlQueue.add(normalized);
                    added++;
                }
            }
        }

        return allOutgoingLinks;
    }

    // Save the page to the database
    private void savePage(String url, String title, String content, String contentHash,
            List<String> outgoingLinks)
    {
        // Only synchronize the critical section
        synchronized (saveLock)
        {
            // Double-check the limit
            if (isMaxPagesReached())
            {
                System.out.println("Max pages reached during save attempt for: " + url);
                return;
            }

            try
            {
                // Create document
                org.bson.Document pageDoc = new org.bson.Document().append("url", url)
                        .append("title", title).append("content", content)
                        .append("contentHash", contentHash).append("outgoingLinks", outgoingLinks)
                        .append("timestamp", System.currentTimeMillis());

                // Insert document
                collection.insertOne(pageDoc);

                // Increment counter after successful save
                int newCount = savedCount.incrementAndGet();
                System.out.println("Saved page " + newCount + "/" + MAX_PAGES + ": " + url);

            }
            catch (Exception e)
            {
                System.err.println("Failed to save to database: " + url + " - " + e.getMessage());
                skippedCount.incrementAndGet();
            }
        }
    }

    // Stop the crawler gracefullypublic void stopCrawling()
    public void stopCrawling()
    {
        isRunning = false;
        printCrawlSummary();
        DatabaseConnection.closeConnection();
        System.out.println("Crawler stopped.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////
    /// load seed urls from file
    private List<String> loadSeedsFromFile(String filePath)
    {
        try
        {
            return Files.readAllLines(Paths.get(filePath));
        }
        catch (IOException e)
        {
            System.err.println(
                    "Failed to load seeds from file: " + filePath + " - " + e.getMessage());
            return new ArrayList<>(); // Return empty list on error
        }
    }

    // normalize seed urls add them to the queue
    public void addSeedUrl(String url)
    {
        try
        {
            // Normalize the URL (remove fragments, default ports, etc.)
            String normalized = UrlNormalizer.normalize(url);
            if (normalized != null && !url.isEmpty())
            {
                urlQueue.add(normalized);
                System.out.println("Added seed URL: " + normalized);
            }
        }
        catch (Exception e)
        {
            System.err.println("Failed to add seed URL: " + url + " - " + e.getMessage());
        }
    }

    // Centralizing MAX_PAGES check
    private boolean isMaxPagesReached()
    {
        synchronized (saveLock)
        {
            return savedCount.get() >= MAX_PAGES;
        }
    }

    // Check if the page content is a duplicate based on its hash
    private boolean isDuplicate(String hash)
    {
        try
        {
            return collection.find(Filters.eq("contentHash", hash)).first() != null;
        }
        catch (Exception e)
        {
            System.err.println("Database error checking for duplicate: " + e.getMessage());
            return false; // Assume not duplicate on error to continue crawling
        }
    }

    // Print a summary of the crawl statisticsprivate void printCrawlSummary()
    private void printCrawlSummary()
    {
        System.out.println("\n--- CRAWL SUMMARY ---");
        System.out.println("Pages saved to database: " + savedCount.get() + "/" + MAX_PAGES);
        System.out.println("Pages attempted: " + attemptedCount.get());
        System.out.println("Pages skipped: " + skippedCount.get());
        System.out.println("URLs in queue: " + urlQueue.size());
        System.out.println("URLs visited: " + visitedUrls.size());
        System.out.println("--------------------\n");
    }

    // Log the progress of the crawler
    private void logProgress()
    {
        System.out.println("Progress: Saved pages = " + savedCount.get() + ", Attempted pages = "
                + attemptedCount.get() + ", Skipped pages = " + skippedCount.get());
    }

    ////////////////////////////////////////////////////////////////////////////////////////
    // Main method to run the crawler
    public static void main(String[] args)
    {
        int maxPages = 200; // Maximum pages to save
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.min(availableProcessors, 12);
        String seedsFile = "data/seeds.txt"; // Path to the seed URLs file

        WebCrawler crawler = new WebCrawler(seedsFile, maxPages, threadCount);

        // Add shutdown hook to stop crawler gracefully on JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(crawler::stopCrawling));
        crawler.startCrawling();
    }
}
