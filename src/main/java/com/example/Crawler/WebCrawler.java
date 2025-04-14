// package com.example.Crawler;

// import com.mongodb.client.MongoCollection;
// import com.example.DatabaseConnection;
// import com.mongodb.client.model.Filters;
// import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
// import org.jsoup.nodes.Element;
// import org.jsoup.select.Elements;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.concurrent.atomic.AtomicInteger;
// import java.io.IOException;
// import java.net.URL;

// public class WebCrawler {
//     private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
//     private final PagePriority priorityQueue = new PagePriority();
//     private final int MAX_PAGES;
//     private final int THREAD_COUNT;
//     private final MongoCollection<org.bson.Document> collection;
//     private final AtomicInteger crawledCount = new AtomicInteger(0);
//     private final DomainRateLimiter rateLimiter = new DomainRateLimiter();
//     private volatile boolean isRunning = true;

//     public WebCrawler(String seedUrl, int maxPages, int threadCount) {
//         this.MAX_PAGES = maxPages;
//         this.THREAD_COUNT = threadCount;
//         this.collection = DatabaseConnection.getDatabase().getCollection("pages");
//         addSeedUrl(seedUrl);
//     }

//     private void addSeedUrl(String url) {
//         String normalized = UrlNormalizer.normalize(url);
//         if (normalized != null) {
//             priorityQueue.addPage(normalized, 100);
//         }
//     }



//     public void startCrawling() {
//         ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
//         try {
//             System.out.println("Starting crawling with initial queue size: " + priorityQueue.size());
//             // while (isRunning && crawledCount.get() < MAX_PAGES) {
//             //     try {
//             //         System.out.println("Waiting for next page, queue size: " + priorityQueue.size());
//             //         PagePriority.WebPage page = priorityQueue.getNextPage();
//             //         if (crawledCount.get() >= MAX_PAGES) {
//             //             System.out.println("Max pages limit reached, stopping task submission.");
//             //             break;
//             //         }
//             //         System.out.println("Processing page: " + page.url);
//             //         executor.submit(() -> {
//             //             try {
//             //                 processPage(page);
//             //             } catch (Exception e) {
//             //                 System.err.println("Task failed for " + page.url + ": " + e.getMessage());
//             //                 e.printStackTrace();
//             //             }
//             //         });
//             //     } catch (InterruptedException e) {
//             //         Thread.currentThread().interrupt();
//             //         break;
//             //     }
//             // }
//             while (isRunning) {
//                 if (crawledCount.get() >= MAX_PAGES) {
//                     System.out.println("Reached max pages, exiting crawl loop.");
//                     break;
//                 }
            
//                 PagePriority.WebPage page = priorityQueue.pollPage(); // Non-blocking version
//                 if (page == null) {
//                     // Thread.sleep(200); // wait a bit for new pages to be added
//                     try {
//                         Thread.sleep(200);
//                     } catch (InterruptedException e) {
//                         Thread.currentThread().interrupt(); // Restore interrupted status
//                         System.err.println("Sleep interrupted: " + e.getMessage());
//                     }
//                     continue;
//                 }
            
//                 executor.submit(() -> {
//                     try {
//                         processPage(page);
//                     } catch (Exception e) {
//                         System.err.println("Task failed for " + page.url + ": " + e.getMessage());
//                         e.printStackTrace();
//                     }
//                 });
//             }
            
//             System.out.println("Crawling stopped. Total pages crawled: " + crawledCount.get() + ", MAX_PAGES: " + MAX_PAGES);
//         } finally {
//             executor.shutdown();
//             try {
//                 if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
//                     executor.shutdownNow();
//                 }
//             } catch (InterruptedException e) {
//                 executor.shutdownNow();
//                 Thread.currentThread().interrupt();
//             }
//         }
//     }

//     public void stopCrawling() {
//         isRunning = false;
//         DatabaseConnection.closeConnection(); // Close MongoDB client
//         System.out.println("Crawler stopped.");
//     }

 
//     private void processPage(PagePriority.WebPage page) {
//         // Safeguard: Stop processing if max pages reached
//         if (crawledCount.get() >= MAX_PAGES) {
//             System.out.println("Max pages limit reached (" + MAX_PAGES + "), skipping: " + page.url);
//             return;
//         }
    
//         // Increment crawledCount early, before any long-running operations
//         int count = crawledCount.incrementAndGet();
//         if (count > MAX_PAGES) {
//             System.out.println("Exceeded max pages during processing: " + page.url + ", count: " + count);
//             return;
//         }
    
//         if (!visitedUrls.add(page.url)) {
//             System.out.println("Already visited: " + page.url);
//             return;
//         }
    
//         try {
//             System.out.println("Checking robots.txt for: " + page.url);
//             if (!RobotsChecker.isAllowed(page.url)) {
//                 System.out.println("Disallowed by robots.txt: " + page.url);
//                 return;
//             }
    
//             System.out.println("Waiting for rate limiter: " + page.url);
//             rateLimiter.waitForDomain(page.url);
    
//             System.out.println("Fetching page: " + page.url);
//             int maxRetries = 3;
//             Document doc = null;
//             for (int i = 0; i < maxRetries; i++) {
//                 try {
//                     doc = Jsoup.connect(page.url)
//                                .userAgent(RobotsChecker.USER_AGENT)
//                                .timeout(5000)
//                                .get();
//                     break;
//                 } catch (IOException e) {
//                     if (i == maxRetries - 1) {
//                         System.err.println("Failed to fetch page after " + maxRetries + " attempts: " + page.url);
//                         return;
//                     }
//                     System.out.println("Retrying page fetch for " + page.url + " (attempt " + (i + 1) + ")");
//                     try {
//                         Thread.sleep(1000);
//                     } catch (InterruptedException ie) {
//                         Thread.currentThread().interrupt();
//                     }
//                 }
//             }
//             if (doc == null) return;
    
//             System.out.println("Checking content type: " + page.url);
//             if (!isHtmlContent(doc)) {
//                 System.out.println("Not HTML content: " + page.url);
//                 return;
//             }
    
//             String content = doc.body().text();
//             String title = doc.title();
//             System.out.println("Generating content hash: " + page.url);
//             String contentHash = PageHasher.generateHash(content);
    
//             System.out.println("Processing links: " + page.url);
//             processLinks(doc, page.url);
    
//             System.out.println("Checking for duplicates: " + page.url);
//             if (isDuplicate(contentHash)) {
//                 System.out.println("Duplicate content: " + page.url);
//                 return;
//             }
    
//             System.out.println("Saving page to MongoDB: " + page.url);
//             savePage(page.url, title, content, contentHash, extractLinks(doc));
    
//             System.out.printf("Crawled %d/%d: %s\n", count, MAX_PAGES, page.url);
//         } catch (Exception e) {
//             System.err.println("Error crawling " + page.url + ": " + e.getMessage());
//             e.printStackTrace();
//         }
//     }

//     private boolean isHtmlContent(Document doc) {
//         String contentType = doc.connection().response().contentType();
//         return contentType != null && contentType.startsWith("text/html");
//     }

//     private void processLinks(Document doc, String sourceUrl) {
//         Elements links = doc.select("a[href]");
//         int added = 0;
//         for (Element link : links) {
//             String nextUrl = link.absUrl("href");
//             String normalized = UrlNormalizer.normalize(nextUrl);
            
//             if (normalized != null && !visitedUrls.contains(normalized)) {
//                 int score = calculateLinkScore(link, sourceUrl, normalized);
//                 priorityQueue.addPage(normalized, score);
//                 added++;
//             }
//         }
//         System.out.println("Added " + added + " new URLs to queue from " + sourceUrl);
//     }

//     private List<String> extractLinks(Document doc) {
//         List<String> links = new ArrayList<>();
//         for (Element link : doc.select("a[href]")) {
//             String absUrl = link.absUrl("href");
//             if (!absUrl.isEmpty()) {
//                 links.add(absUrl);
//             }
//         }
//         return links;
//     }

//     private int calculateLinkScore(Element link, String sourceUrl, String targetUrl) {
//         int score = 10; // Base score
        
//         try {
//             URL source = new URL(sourceUrl);
//             URL target = new URL(targetUrl);
            
//             if (source.getHost().equals(target.getHost())) {
//                 score += 20; // Same domain bonus
//             }
            
//             if (link.hasAttr("title")) {
//                 score += 5; // Title attribute bonus
//             }
            
//             String linkText = link.text().trim();
//             if (!linkText.isEmpty() && linkText.length() < 50) {
//                 score += linkText.length(); // Longer text = more important
//             }
            
//             if (link.parent() != null && "nav".equals(link.parent().tagName())) {
//                 score -= 10; // Navigation links often less important
//             }
            
//         } catch (Exception e) {
//             // Keep base score if URL parsing fails
//         }
        
//         return Math.max(1, score); // Ensure minimum score of 1
//     }

//     private boolean isDuplicate(String hash) {
//         return collection.find(Filters.eq("contentHash", hash)).first() != null;
//     }

//     private void savePage(String url, String title, String content, 
//                         String hash, List<String> outgoingLinks) {
//         org.bson.Document pageDoc = new org.bson.Document()
//             .append("url", url)
//             .append("title", title)
//             .append("content", content)
//             .append("contentHash", hash)
//             .append("outgoingLinks", outgoingLinks)
//             .append("timestamp", System.currentTimeMillis());
        
//         collection.insertOne(pageDoc);
//     }

//     public static void main(String[] args) {
//         WebCrawler crawler = new WebCrawler("https://www.w3schools.com", 10, 3);
//         crawler.addSeedUrl("https://www.geeksforgeeks.org");
//         crawler.addSeedUrl("https://www.tutorialspoint.com");
//         Runtime.getRuntime().addShutdownHook(new Thread(crawler::stopCrawling));
//         crawler.startCrawling();
//     }
// }

// class DomainRateLimiter {
//     private final ConcurrentMap<String, RateLimiter> limiters = new ConcurrentHashMap<>();
//     private static final long DEFAULT_DELAY = 1000;
    
//     public void waitForDomain(String url) {
//         try {
//             String domain = new URL(url).getHost();
//             RateLimiter limiter = limiters.computeIfAbsent(domain, 
//                 k -> new RateLimiter(RobotsChecker.getCrawlDelay(url)));
//             limiter.acquire();
//         } catch (Exception e) {
//             try {
//                 Thread.sleep(DEFAULT_DELAY);
//             } catch (InterruptedException ie) {
//                 Thread.currentThread().interrupt();
//             }
//         }
//     }
    
//     static class RateLimiter {
//         private final long delay;
//         private long lastAccess;
        
//         public RateLimiter(long delay) {
//             this.delay = delay;
//         }
        
//         public synchronized void acquire() throws InterruptedException {
//             long now = System.currentTimeMillis();
//             long elapsed = now - lastAccess;
            
//             if (elapsed < delay) {
//                 Thread.sleep(delay - elapsed);
//             }
            
//             lastAccess = System.currentTimeMillis();
//         }
//     }
// }
package com.example.Crawler;

import com.mongodb.client.MongoCollection;
import com.example.DatabaseConnection;
import com.mongodb.client.model.Filters;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.net.URL;

public class WebCrawler {
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<String> urlQueue = new LinkedBlockingQueue<>();
    private final int MAX_PAGES;
    private final int THREAD_COUNT;
    private final MongoCollection<org.bson.Document> collection;
    private final AtomicInteger savedCount = new AtomicInteger(0);
    private final AtomicInteger attemptedCount = new AtomicInteger(0);
    private final AtomicInteger skippedCount = new AtomicInteger(0);
    private final DomainRateLimiter rateLimiter = new DomainRateLimiter();
    private volatile boolean isRunning = true;
    private final Object saveLock = new Object(); // Lock for synchronizing save operations

    private static final String[] DEFAULT_SEEDS = {
        "https://www.w3schools.com",
        "https://www.geeksforgeeks.org",
        "https://www.tutorialspoint.com",
        "https://developer.mozilla.org",
        "https://stackoverflow.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.github.com",
        "https://news.ycombinator.com",
        "https://medium.com",
        "https://www.bbc.com",
        "https://www.cnn.com",
        "https://www.theguardian.com",
        "https://www.forbes.com",
        "https://www.techcrunch.com"
    };

    public WebCrawler(int maxPages, int threadCount) {
        this.MAX_PAGES = maxPages;
        this.THREAD_COUNT = threadCount;
        this.collection = DatabaseConnection.getDatabase().getCollection("pages");
        
        for (String seed : DEFAULT_SEEDS) {
            addSeedUrl(seed);
        }
    }

    public WebCrawler(String seedUrl, int maxPages, int threadCount) {
        this(maxPages, threadCount);
        addSeedUrl(seedUrl);
    }

    public void addSeedUrl(String url) {
        try {
            String normalized = UrlNormalizer.normalize(url);
            if (normalized != null && !url.isEmpty()) {
                urlQueue.add(normalized);
                System.out.println("Added seed URL: " + normalized);
            }
        } catch (Exception e) {
            System.err.println("Failed to add seed URL: " + url + " - " + e.getMessage());
        }
    }

    public void startCrawling() {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    crawlWorker();
                } catch (Exception e) {
                    System.err.println("Worker thread error: " + e.getMessage());
                    e.printStackTrace();
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
            // Synchronize the check to prevent overshooting
            synchronized (saveLock) {
                if (savedCount.get() >= MAX_PAGES) {
                    System.out.println("Thread " + Thread.currentThread().getName() + ": Max pages reached, stopping.");
                    break;
                }
            }

            String url = null;
            try {
                url = urlQueue.poll(5, TimeUnit.SECONDS);
                
                if (url == null) {
                    if (urlQueue.isEmpty() && savedCount.get() < MAX_PAGES) {
                        System.out.println("Queue empty, waiting for new URLs... Saved: " + savedCount.get());
                        Thread.sleep(5000);
                        if (urlQueue.isEmpty() && savedCount.get() < MAX_PAGES) {
                            System.out.println("Queue still empty, attempting to add more seeds...");
                            for (String seed : DEFAULT_SEEDS) {
                                String normalized = UrlNormalizer.normalize(seed);
                                if (normalized != null && !visitedUrls.contains(normalized) && !urlQueue.contains(normalized)) {
                                    urlQueue.add(normalized);
                                    System.out.println("Re-added seed URL: " + normalized);
                                }
                            }
                        }
                        continue;
                    }
                    continue;
                }
                
                String normalizedUrl = UrlNormalizer.normalize(url);
                if (normalizedUrl == null || !visitedUrls.add(normalizedUrl)) {
                    continue;
                }
                
                attemptedCount.incrementAndGet();
                processPage(normalizedUrl);
                
                int saved = savedCount.get();
                int attempted = attemptedCount.get();
                int skipped = skippedCount.get();
                System.out.printf("Progress: Saved %d/%d | Attempted: %d | Skipped: %d | Queue: %d\n", 
                                 saved, MAX_PAGES, attempted, skipped, urlQueue.size());
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Error processing URL: " + url + " - " + e.getMessage());
            }
        }
    }

    public void stopCrawling() {
        isRunning = false;
        printCrawlSummary();
        DatabaseConnection.closeConnection();
        System.out.println("Crawler stopped.");
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
 
    private void processPage(String url) {
        try {
            if (!RobotsChecker.isAllowed(url)) {
                System.out.println("Disallowed by robots.txt: " + url);
                skippedCount.incrementAndGet();
                return;
            }
            
            rateLimiter.waitForDomain(url);
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted during rate limiter wait: " + url);
                return;
            }
            
            Document doc = fetchWithRetries(url);
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
            String contentHash = PageHasher.generateHash(content);
            
            if (isDuplicate(contentHash)) {
                System.out.println("Duplicate content: " + url);
                skippedCount.incrementAndGet();
                return;
            }
            
            queueNewUrls(doc);
            
            // Synchronize the save operation to prevent overshooting
            synchronized (saveLock) {
                if (savedCount.get() >= MAX_PAGES) {
                    System.out.println("Max pages reached during save attempt for: " + url);
                    return;
                }
                
                boolean saved = savePage(url, title, content, contentHash, extractLinks(doc));
                if (saved) {
                    int newCount = savedCount.incrementAndGet();
                    System.out.println("Saved page " + newCount + "/" + MAX_PAGES + ": " + url);
                } else {
                    System.out.println("Failed to save page to database: " + url);
                    skippedCount.incrementAndGet();
                }
            }
            
        } catch (Exception e) {
            System.err.println("Failed to process " + url + ": " + e.getMessage());
            skippedCount.incrementAndGet();
        }
    }
    
    private boolean isHtmlContent(Document doc) {
        String contentType = doc.connection().response().contentType();
        return contentType != null && contentType.toLowerCase().startsWith("text/html");
    }
    
    private Document fetchWithRetries(String url) {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted during fetch: " + url);
                return null;
            }
            try {
                return Jsoup.connect(url)
                           .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36")
                           .timeout(10000)
                           .followRedirects(true)
                           .get();
            } catch (IOException e) {
                System.err.println("Attempt " + (i+1) + " failed for " + url + ": " + e.getMessage());
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

    private void queueNewUrls(Document doc) {
        Elements links = doc.select("a[href]");
        int added = 0;
        
        for (Element link : links) {
            if (added >= 50) break;
            
            String nextUrl = link.absUrl("href");
            String normalized = UrlNormalizer.normalize(nextUrl);
            if (normalized != null && !normalized.isEmpty() && 
                normalized.startsWith("http") && 
                !visitedUrls.contains(normalized) &&
                !urlQueue.contains(normalized)) {
                
                try {
                    urlQueue.add(normalized);
                    added++;
                } catch (Exception e) {
                    System.out.println("Failed to add URL to queue: " + normalized);
                }
            }
        }
        System.out.println("Added " + added + " new URLs from " + doc.location());
    }

    private List<String> extractLinks(Document doc) {
        List<String> links = new ArrayList<>();
        for (Element link : doc.select("a[href]")) {
            String absUrl = link.absUrl("href");
            if (!absUrl.isEmpty()) {
                links.add(absUrl);
            }
        }
        return links;
    }

    private boolean isDuplicate(String hash) {
        try {
            return collection.find(Filters.eq("contentHash", hash)).first() != null;
        } catch (Exception e) {
            System.err.println("Database error checking for duplicate: " + e.getMessage());
            return false;
        }
    }

    private boolean savePage(String url, String title, String content, 
                           String hash, List<String> outgoingLinks) {
        // Final check before saving
        if (savedCount.get() >= MAX_PAGES) {
            System.out.println("Max pages already reached, skipping save for: " + url);
            return false;
        }

        org.bson.Document pageDoc = new org.bson.Document()
            .append("url", url)
            .append("title", title)
            .append("content", content)
            .append("contentHash", hash)
            .append("outgoingLinks", outgoingLinks)
            .append("timestamp", System.currentTimeMillis());
        
        try {
            collection.insertOne(pageDoc);
            System.out.println("Successfully saved: " + url);
            return true;
        } catch (Exception e) {
            System.err.println("Failed to save to database: " + url + " - " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
        int maxPages = 10;
        int threadCount = 5;
        
        WebCrawler crawler = new WebCrawler(maxPages, threadCount);
        Runtime.getRuntime().addShutdownHook(new Thread(crawler::stopCrawling));
        crawler.startCrawling();
    }
}

class DomainRateLimiter {
    private final ConcurrentMap<String, RateLimiter> limiters = new ConcurrentHashMap<>();
    private static final long DEFAULT_DELAY = 1000;
    
    public void waitForDomain(String url) {
        try {
            String domain = new URL(url).getHost();
            RateLimiter limiter = limiters.computeIfAbsent(domain, 
                k -> new RateLimiter(RobotsChecker.getCrawlDelay(url)));
            limiter.acquire();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("Rate limiter interrupted for URL: " + url);
        } catch (Exception e) {
            try {
                Thread.sleep(DEFAULT_DELAY);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    static class RateLimiter {
        private final long delay;
        private long lastAccess;
        
        public RateLimiter(long delay) {
            this.delay = Math.max(delay, 500);
        }
        
        public synchronized void acquire() throws InterruptedException {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Rate limiter interrupted");
            }
            long now = System.currentTimeMillis();
            long elapsed = now - lastAccess;
            
            if (elapsed < delay) {
                Thread.sleep(delay - elapsed);
            }
            
            lastAccess = System.currentTimeMillis();
        }
    }
}