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

    private final List<org.bson.Document> pageBuffer =
            Collections.synchronizedList(new ArrayList<>());
    private final int BATCH_SIZE = 200;
    private final Object batchLock = new Object();

    private final Set<String> contentHashCache =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final int MAX_HASH_CACHE = 10000;

    private Map<String, Queue<String>> domainQueues = new ConcurrentHashMap<>();
    private Queue<String> domainRotation = new ConcurrentLinkedQueue<>();

    private final int MAX_LINKS_PER_PAGE = 35;

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
        ExecutorService executor = Executors.newWorkStealingPool(THREAD_COUNT);
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
                url = getNextUrl(); // Get next URL using domain rotation logic
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

    private String getNextUrl()
    {
        // Add timeout to domain polling
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 3000) // 3 second timeout
        {
            // First try to get a URL through domain rotation
            if (!domainRotation.isEmpty())
            {
                // Get next domain in rotation
                String domain = domainRotation.poll();

                // Get a URL from that domain if available
                Queue<String> domainQueue = domainQueues.get(domain);
                String url = domainQueue.poll();

                // Put domain back in rotation if it has more URLs
                if (!domainQueue.isEmpty())
                {
                    domainRotation.add(domain);
                }

                if (url != null)
                {
                    return url;
                }

                // Try another domain if current one is taking too long
                if (!domainRotation.isEmpty() && System.currentTimeMillis() - startTime > 500)
                {
                    continue;
                }
            }
        }

        // Fall back to regular queue if domain rotation is empty
        try
        {
            return urlQueue.poll(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
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
            rateLimiter.waitForDomain(url); // The delay here could be adjusted
            if (Thread.currentThread().isInterrupted())
            {
                System.out.println("Interrupted during rate limiter wait: " + url);
                return;
            }

            // Fetch the page content with retries
            long fetchStart = System.currentTimeMillis();
            Document doc = fetchWithRetries(url);
            long fetchTime = System.currentTimeMillis() - fetchStart;

            // Track domain response time
            String domain = UrlNormalizer.extractDomain(url);
            if (doc != null && domain != null)
            {
                // Update domain statistics for rate limiter
                rateLimiter.recordSuccess(domain, fetchTime);

                // Every 20 pages, shuffle the domain rotation to avoid getting stuck
                if (savedCount.get() % 20 == 0)
                {
                    synchronized (domainRotation)
                    {
                        List<String> domains = new ArrayList<>(domainRotation);
                        Collections.shuffle(domains);
                        domainRotation.clear();
                        domainRotation.addAll(domains);
                    }
                }
            }

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
                return Jsoup.connect(url).userAgent("Mozilla/5.0...").timeout(3000)
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
            if (added < MAX_LINKS_PER_PAGE)
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
        synchronized (batchLock)
        {
            // Create unique document ID if needed
            String docId = UUID.randomUUID().toString();

            // Create document with indexed=false flag
            org.bson.Document pageDoc = new org.bson.Document().append("url", url)
                    .append("title", title).append("content", content)
                    .append("contentHash", contentHash).append("doc_id", docId)
                    .append("outgoingLinks", outgoingLinks).append("indexed", false)
                    .append("timestamp", System.currentTimeMillis());

            pageBuffer.add(pageDoc);
            int newCount = savedCount.incrementAndGet();

            // Only process the batch when it reaches the threshold
            if (pageBuffer.size() >= BATCH_SIZE || newCount >= MAX_PAGES)
                flushPageBuffer();
            else
                System.out.println("Buffered page " + newCount + "/" + MAX_PAGES + ": " + url);
        }
    }

    // Add this new method for batch inserts
    private void flushPageBuffer()
    {
        if (pageBuffer.isEmpty())
            return;

        List<org.bson.Document> batch;
        synchronized (batchLock)
        {
            batch = new ArrayList<>(pageBuffer);
            pageBuffer.clear();
        }
        try
        {
            // Insert all documents in one database operation
            collection.insertMany(batch);
            System.out.println("Batch saved: " + batch.size() + " pages");
        }
        catch (Exception e)
        {
            System.err.println("Failed to save batch to database: " + e.getMessage());
            // Decrement savedCount for failed saves
            savedCount.addAndGet(-batch.size());
        }
    }

    // Stop the crawler gracefully
    public void stopCrawling()
    {
        isRunning = false;
        flushPageBuffer(); // Ensure any remaining pages are saved
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
                String domain = UrlNormalizer.extractDomain(normalized);

                // Add domain to rotation if new
                if (!domainQueues.containsKey(domain))
                {
                    domainQueues.put(domain, new ConcurrentLinkedQueue<>());
                    domainRotation.add(domain);
                }

                // Add URL to its domain queue
                domainQueues.get(domain).add(normalized);
                System.out.println("Added seed URL: " + normalized + " to domain: " + domain);
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
        // AtomicInteger is already thread-safe
        return savedCount.get() >= MAX_PAGES;
    }

    // Check if the page content is a duplicate based on its hash
    private boolean isDuplicate(String hash)
    {
        // Check in-memory cache first (much faster)
        if (contentHashCache.contains(hash))
            return true;
        // Only check database if not in cache
        try
        {
            org.bson.Document doc = collection.find(Filters.eq("contentHash", hash)).first();
            if (doc != null)
            {
                // Add to cache for future checks
                if (contentHashCache.size() < MAX_HASH_CACHE)
                    contentHashCache.add(hash);
                return true;
            }
            return false;
        }
        catch (Exception e)
        {
            System.err.println("Database error checking for duplicate: " + e.getMessage());
            return false;
        }
    }

    // Print a summary of the crawl statistics
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
        int maxPages = 100; // Maximum pages to save
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.min(availableProcessors, 12);
        String seedsFile = "data/seeds.txt"; // Path to the seed URLs file

        WebCrawler crawler = new WebCrawler(seedsFile, maxPages, threadCount);

        // Add shutdown hook to stop crawler gracefully on JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(crawler::stopCrawling));
        crawler.startCrawling();
    }
}
