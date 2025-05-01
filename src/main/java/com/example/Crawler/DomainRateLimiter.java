package com.example.Crawler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

// manage rate limiting for different domains Ensures we don't overwhelm servers
// with too many requests
public class DomainRateLimiter
{
    // Map of domain-specific rate limiters
    private static final long DEFAULT_DELAY = 500; // Default 1 second delay

    // Add these fields to DomainRateLimiter
    private final Map<String, Integer> domainResponseTimes = new ConcurrentHashMap<>();
    private final Map<String, Long> domainSuccessCount = new ConcurrentHashMap<>();
    private Map<String, Long> lastAccessTimes = new ConcurrentHashMap<>();

    // In your waitForDomain method, add this adaptive timing
    public void waitForDomain(String url) throws InterruptedException
    {
        String domain = UrlNormalizer.extractDomain(url);
        if (domain == null)
            return;

        // Get domain statistics
        long successCount = domainSuccessCount.getOrDefault(domain, 0L);
        int avgResponseTime = domainResponseTimes.getOrDefault(domain, 0);

        // Calculate adaptive delay (faster for responsive sites)
        long adaptiveDelay = DEFAULT_DELAY;
        if (successCount > 5)
        {
            // Reduce delay for consistently fast domains
            if (avgResponseTime < 500)
            {
                adaptiveDelay = 200; // Much faster for responsive domains
            }
            else if (avgResponseTime > 2000)
            {
                adaptiveDelay = Math.min(2000, DEFAULT_DELAY); // Cap delay for slow domains
            }
        }

        // Apply delay
        domain = domain.trim().toLowerCase();
        Long lastAccess = lastAccessTimes.get(domain);
        long currentTime = System.currentTimeMillis();

        if (lastAccess != null)
        {
            long timeSinceLastAccess = currentTime - lastAccess;
            if (timeSinceLastAccess < adaptiveDelay)
            {
                Thread.sleep(adaptiveDelay - timeSinceLastAccess);
            }
        }

        lastAccessTimes.put(domain, System.currentTimeMillis());
    }


    // Records a successful request to a domain with its response time Used for adaptive rate
    // limiting
    public void recordSuccess(String domain, long responseTimeMs)
    {
        if (domain == null)
            return;

        domain = domain.trim().toLowerCase();

        // Update response time tracking
        Integer currentAvg = domainResponseTimes.getOrDefault(domain, 0);
        int newCount = domainSuccessCount.getOrDefault(domain, 0L).intValue() + 1;

        // Calculate new moving average (weight recent responses more)
        int newAvg;
        if (newCount <= 1)
        {
            newAvg = (int) responseTimeMs;
        }
        else
        {
            // Exponential moving average with 0.3 weight for new value
            newAvg = (int) (currentAvg * 0.7 + responseTimeMs * 0.3);
        }

        domainResponseTimes.put(domain, newAvg);
        domainSuccessCount.put(domain, (long) newCount);

        // Optional: print stats every 10 successes
        if (newCount % 10 == 0)
        {
            System.out.println("Domain stats - " + domain + ": " + newCount
                    + " successes, avg response: " + newAvg + "ms");
        }
    }

    // Rate limiter for a specific domain
    static class RateLimiter
    {
        private final long delay; // Minimum time between requests
        private long lastAccess; // Time of last request

        // Constructor for RateLimiter
        public RateLimiter(long delay)
        {
            this.delay = Math.max(delay, 500); // Enforce minimum 500ms delay
        }

        // Acquire the rate limit for this domain
        public synchronized void acquire() throws InterruptedException
        {
            // Check for thread interruption
            if (Thread.currentThread().isInterrupted())
            {
                throw new InterruptedException("Rate limiter interrupted");
            }

            long now = System.currentTimeMillis();
            long elapsed = now - lastAccess;

            // If not enough time has passed, sleep for the remaining time
            if (elapsed < delay)
            {
                Thread.sleep(delay - elapsed);
            }

            // Update last access time
            lastAccess = System.currentTimeMillis();
        }
    }
}
