package com.example.Crawler;

import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// manage rate limiting for different domains Ensures we don't overwhelm servers
// with too many requests
public class DomainRateLimiter
{
    // Map of domain-specific rate limiters
    private final ConcurrentMap<String, RateLimiter> limiters = new ConcurrentHashMap<>();
    private static final long DEFAULT_DELAY = 1000; // Default 1 second delay

    /// Wait for the rate limit for a specific domain
    public void waitForDomain(String url)
    {
        try
        {
            String domain = new URL(url).getHost(); // Extract domain from URL

            // Get or create rate limiter for this domain
            RateLimiter limiter = limiters.computeIfAbsent(domain,
                    k -> new RateLimiter(RobotsChecker.getCrawlDelay(url)));
            limiter.acquire(); // Wait for rate limit
        }
        catch (InterruptedException ie)
        {
            Thread.currentThread().interrupt();
            System.out.println("Rate limiter interrupted for URL: " + url);
        }
        catch (Exception e)
        {
            // If we can't parse the URL or get the rate limiter, use default delay
            try
            {
                Thread.sleep(DEFAULT_DELAY);
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
            }
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
