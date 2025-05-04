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
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

public class WebCrawler {
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<String> urlQueue = new LinkedBlockingQueue<>();
    private final int MAX_PAGES;
    private final int THREAD_COUNT;
    private final String DEFAULT_SEEDS_FILE;
    private final MongoCollection<Document> collection;

    // Counters to track crawler statistics
    private final AtomicInteger savedCount = new AtomicInteger(0);
    private final AtomicInteger attemptedCount = new AtomicInteger(0);
    private final AtomicInteger skippedCount = new AtomicInteger(0);
    private final DomainRateLimiter rateLimiter = new DomainRateLimiter();

    private volatile boolean isRunning = true;
    private final List<Document> pageBuffer = Collections.synchronizedList(new ArrayList<>());
    private final int BATCH_SIZE = 200;
    private final Object batchLock = new Object();
    private final Set<String> contentHashCache = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final int MAX_HASH_CACHE = 10000;
    private final Map<String, Queue<String>> domainQueues = new ConcurrentHashMap<>();
    private final Queue<String> domainRotation = new ConcurrentLinkedQueue<>();
    private final int MAX_LINKS_PER_PAGE = 35;

    public WebCrawler(String seedsFile, int maxPages, int threadCount) {
        this.MAX_PAGES = maxPages;
        this.THREAD_COUNT = threadCount;
        DEFAULT_SEEDS_FILE = seedsFile;
        this.collection = DatabaseConnection.getDatabase().getCollection("pages");
        
        // Create indexes when initializing the crawler
        createIndexes();
        
        // Load seed URLs from file
        List<String> seedUrls = loadSeedsFromFile(DEFAULT_SEEDS_FILE);
        for (String seed : seedUrls) {
            addSeedUrl(seed);
        }
    }

    private void createIndexes() {
        try {
            // Create index on doc_id (unique identifier)
            collection.createIndex(Indexes.ascending("doc_id"), 
                new IndexOptions().unique(true).name("doc_id_index"));
            
            // Create index on url (for URL-based lookups)
            collection.createIndex(Indexes.ascending("url"), 
                new IndexOptions().name("url_index"));
            
            // Create index on contentHash (for duplicate detection)
            collection.createIndex(Indexes.ascending("contentHash"), 
                new IndexOptions().name("content_hash_index"));
            
            // Create text index for full-text search
            collection.createIndex(Indexes.compoundIndex(
                Indexes.text("title"), Indexes.text("content")), 
                new IndexOptions().name("text_search_index"));
            
            // Create index on timestamp for time-based queries
            collection.createIndex(Indexes.descending("timestamp"), 
                new IndexOptions().name("timestamp_index"));
            
            System.out.println("Created database indexes for optimal performance");
        } catch (Exception e) {
            System.err.println("Failed to create indexes: " + e.getMessage());
        }
    }

    public void startCrawling() {
        ExecutorService executor = Executors.newWorkStealingPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    crawlWorker();
                } catch (Exception e) {
                    System.err.println("Worker thread error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            boolean completed = latch.await(30, TimeUnit.MINUTES);
            if (!completed) {
                System.out.println("Crawler timed out after 30 minutes");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            stopCrawling();
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Executor did not terminate cleanly");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void crawlWorker() {
        while (isRunning) {
            if (isMaxPagesReached()) {
                System.out.println(Thread.currentThread().getName() + ": Max pages reached, stopping.");
                break;
            }

            String url = null;
            try {
                url = getNextUrl();
                if (url == null) {
                    if (!handleEmptyQueue()) {
                        break;
                    }
                    continue;
                }

                String normalizedUrl = normalizeUrl(url);
                if (normalizedUrl == null || !visitedUrls.add(normalizedUrl)) {
                    continue;
                }

                attemptedCount.incrementAndGet();
                processPage(normalizedUrl);
                logProgress();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Error processing URL: " + url + " - " + e.getMessage());
            }
        }
    }

    private String getNextUrl() {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 3000) {
            if (!domainRotation.isEmpty()) {
                String domain = domainRotation.poll();
                Queue<String> domainQueue = domainQueues.get(domain);
                String url = domainQueue.poll();

                if (!domainQueue.isEmpty()) {
                    domainRotation.add(domain);
                }

                if (url != null) {
                    return url;
                }

                if (!domainRotation.isEmpty() && System.currentTimeMillis() - startTime > 500) {
                    continue;
                }
            }
        }

        try {
            return urlQueue.poll(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private boolean handleEmptyQueue() throws InterruptedException {
        if (urlQueue.isEmpty() && !isMaxPagesReached()) {
            System.out.println(Thread.currentThread().getName() + 
                ": Queue empty, waiting 5s for new URLs... Saved: " + savedCount.get());
            Thread.sleep(5000);

            if (urlQueue.isEmpty()) {
                System.out.println(Thread.currentThread().getName() + 
                    ": Queue still empty after waiting, thread exiting.");
                return false;
            }
        }
        return true;
    }

    private void processPage(String url) {
        try {
            if (!isAllowedByRobots(url)) {
                System.out.println("Disallowed by robots.txt: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            rateLimiter.waitForDomain(url);
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted during rate limiter wait: " + url);
                return;
            }

            long fetchStart = System.currentTimeMillis();
            org.jsoup.nodes.Document doc = fetchWithRetries(url);
            long fetchTime = System.currentTimeMillis() - fetchStart;

            String domain = extractDomain(url);
            if (doc != null && domain != null) {
                rateLimiter.recordSuccess(domain, fetchTime);

                if (savedCount.get() % 20 == 0) {
                    synchronized (domainRotation) {
                        List<String> domains = new ArrayList<>(domainRotation);
                        Collections.shuffle(domains);
                        domainRotation.clear();
                        domainRotation.addAll(domains);
                    }
                }
            }

            if (doc == null) {
                System.out.println("Failed to fetch page: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            if (!isHtmlContent(doc)) {
                System.out.println("Not HTML content: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            String title = doc.title();
            String content = doc.body().text();
            String contentHash = generateContentHash(content);

            if (isDuplicate(contentHash)) {
                System.out.println("Duplicate content: " + url);
                skippedCount.incrementAndGet();
                return;
            }

            List<String> outgoingLinks = processLinks(doc);
            savePage(url, title, content, contentHash, outgoingLinks);
        } catch (Exception e) {
            System.err.println("Failed to process " + url + ": " + e.getMessage());
            skippedCount.incrementAndGet();
        }
    }

    private boolean isHtmlContent(org.jsoup.nodes.Document doc) {
        String contentType = doc.connection().response().contentType();
        return contentType != null && contentType.toLowerCase().startsWith("text/html");
    }

    private org.jsoup.nodes.Document fetchWithRetries(String url) {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted during fetch: " + url);
                return null;
            }
            try {
                return Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                    .timeout(3000)
                    .followRedirects(true)
                    .get();
            } catch (IOException e) {
                System.err.println("Attempt " + (i + 1) + " failed for " + url + ": " + e.getMessage());
                if (i == maxRetries - 1) {
                    return null;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private List<String> processLinks(org.jsoup.nodes.Document doc) {
        Elements links = doc.select("a[href]");
        List<String> allOutgoingLinks = new ArrayList<>();
        int added = 0;

        for (Element link : links) {
            String absUrl = link.absUrl("href");

            if (!absUrl.isEmpty()) {
                allOutgoingLinks.add(absUrl);
            }

            if (added < MAX_LINKS_PER_PAGE) {
                String normalized = normalizeUrl(absUrl);
                if (normalized != null && !normalized.isEmpty() && normalized.startsWith("http")
                        && !visitedUrls.contains(normalized) && !urlQueue.contains(normalized)) {
                    urlQueue.add(normalized);
                    added++;
                }
            }
        }

        return allOutgoingLinks;
    }

    private void savePage(String url, String title, String content, String contentHash,
            List<String> outgoingLinks) {
        synchronized (batchLock) {
            String docId = UUID.randomUUID().toString();

            Document pageDoc = new Document()
                .append("url", url)
                .append("title", title)
                .append("content", content)
                .append("contentHash", contentHash)
                .append("doc_id", docId)
                .append("outgoingLinks", outgoingLinks)
                .append("indexed", false)
                .append("timestamp", System.currentTimeMillis());

            pageBuffer.add(pageDoc);
            int newCount = savedCount.incrementAndGet();

            if (pageBuffer.size() >= BATCH_SIZE || newCount >= MAX_PAGES) {
                flushPageBuffer();
            } else {
                System.out.println("Buffered page " + newCount + "/" + MAX_PAGES + ": " + url);
            }
        }
    }

    private void flushPageBuffer() {
        if (pageBuffer.isEmpty()) {
            return;
        }

        List<Document> batch;
        synchronized (batchLock) {
            batch = new ArrayList<>(pageBuffer);
            pageBuffer.clear();
        }
        try {
            collection.insertMany(batch);
            System.out.println("Batch saved: " + batch.size() + " pages");
        } catch (Exception e) {
            System.err.println("Failed to save batch to database: " + e.getMessage());
            savedCount.addAndGet(-batch.size());
        }
    }

    public void stopCrawling() {
        isRunning = false;
        flushPageBuffer();
        printCrawlSummary();
        DatabaseConnection.closeConnection();
        System.out.println("Crawler stopped.");
    }

    private List<String> loadSeedsFromFile(String filePath) {
        try {
            return Files.readAllLines(Paths.get(filePath));
        } catch (IOException e) {
            System.err.println("Failed to load seeds from file: " + filePath + " - " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public void addSeedUrl(String url) {
        try {
            String normalized = normalizeUrl(url);
            if (normalized != null && !url.isEmpty()) {
                String domain = extractDomain(normalized);

                if (!domainQueues.containsKey(domain)) {
                    domainQueues.put(domain, new ConcurrentLinkedQueue<>());
                    domainRotation.add(domain);
                }

                domainQueues.get(domain).add(normalized);
                System.out.println("Added seed URL: " + normalized + " to domain: " + domain);
            }
        } catch (Exception e) {
            System.err.println("Failed to add seed URL: " + url + " - " + e.getMessage());
        }
    }

    private boolean isMaxPagesReached() {
        return savedCount.get() >= MAX_PAGES;
    }

    private boolean isDuplicate(String hash) {
        if (contentHashCache.contains(hash)) {
            return true;
        }
        try {
            Document doc = collection.find(Filters.eq("contentHash", hash)).first();
            if (doc != null) {
                if (contentHashCache.size() < MAX_HASH_CACHE) {
                    contentHashCache.add(hash);
                }
                return true;
            }
            return false;
        } catch (Exception e) {
            System.err.println("Database error checking for duplicate: " + e.getMessage());
            return false;
        }
    }

    private void printCrawlSummary() {
        System.out.println("\n--- CRAWL SUMMARY ---");
        System.out.println("Pages saved to database: " + savedCount.get() + "/" + MAX_PAGES);
        System.out.println("Pages attempted: " + attemptedCount.get());
        System.out.println("Pages skipped: " + skippedCount.get());
        System.out.println("URLs in queue: " + urlQueue.size());
        System.out.println("URLs visited: " + visitedUrls.size());
        System.out.println("--------------------\n");
    }

    private void logProgress() {
        System.out.println("Progress: Saved pages = " + savedCount.get() + 
            ", Attempted pages = " + attemptedCount.get() + 
            ", Skipped pages = " + skippedCount.get());
    }

    // Utility methods for URL processing
    private String normalizeUrl(String url) {
        if (url == null) return null;
        try {
            // Remove fragments and normalize
            return url.split("#")[0].toLowerCase().replaceAll("/+$", "");
        } catch (Exception e) {
            return null;
        }
    }

    private static String extractDomain(String url) {
        if (url == null) return null;
        try {
            java.net.URI uri = new java.net.URI(url);
            String domain = uri.getHost();
            return domain != null ? domain.toLowerCase() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isAllowedByRobots(String url) {
        // Simple implementation - in production use a proper robots.txt parser
        return true;
    }

    private String generateContentHash(String content) {
        // Simple hash implementation - consider using stronger hashing in production
        return Integer.toString(content.hashCode());
    }

    public static void main(String[] args) {
        int maxPages = 10;
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int threadCount = Math.min(availableProcessors, 12);
        String seedsFile = "data/seeds.txt";

        WebCrawler crawler = new WebCrawler(seedsFile, maxPages, threadCount);
        Runtime.getRuntime().addShutdownHook(new Thread(crawler::stopCrawling));
        crawler.startCrawling();
    }

    // Inner class for domain rate limiting
    private static class DomainRateLimiter {
        private final Map<String, Long> lastRequestTimes = new ConcurrentHashMap<>();
        private final long DELAY_MS = 1000; // 1 second between requests to same domain

        public void waitForDomain(String url) throws InterruptedException {
            String domain;
            domain = extractDomain(url);
            if (domain == null) return;

            synchronized (lastRequestTimes) {
                Long lastTime = lastRequestTimes.get(domain);
                long currentTime = System.currentTimeMillis();
                if (lastTime != null) {
                    long timeSinceLast = currentTime - lastTime;
                    if (timeSinceLast < DELAY_MS) {
                        Thread.sleep(DELAY_MS - timeSinceLast);
                    }
                }
                lastRequestTimes.put(domain, System.currentTimeMillis());
            }
        }

        public void recordSuccess(String domain, long fetchTime) {
            // Could adjust delays based on fetch time if needed
        }
    }
}