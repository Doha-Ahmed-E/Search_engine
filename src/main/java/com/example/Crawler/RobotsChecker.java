package com.example.Crawler;

import org.jsoup.Jsoup;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class RobotsChecker {
    private static final ConcurrentMap<String, RobotsTxtCache> domainCache = new ConcurrentHashMap<>();
    public static final String USER_AGENT = "MyWebCrawler/1.0";
    private static final long DEFAULT_DELAY = 1000; // 1 second
    
    public static boolean isAllowed(String url) {
        try {
            String domain = getDomainKey(url);
            RobotsTxtCache cache = getRobotsTxtCache(domain);
            return cache.isAllowed(getPath(url));
        } catch (Exception e) {
            return true; // Default allow if error occurs
        }
    }
    
    public static long getCrawlDelay(String url) {
        try {
            String domain = getDomainKey(url);
            RobotsTxtCache cache = getRobotsTxtCache(domain);
            return cache.getCrawlDelay();
        } catch (Exception e) {
            return DEFAULT_DELAY;
        }
    }
    
    private static RobotsTxtCache getRobotsTxtCache(String domain) throws IOException {
        return domainCache.computeIfAbsent(domain, k -> {
            try {
                String robotsTxt = fetchRobotsTxt(domain);
                return new RobotsTxtCache(robotsTxt);
            } catch (IOException e) {
                return new RobotsTxtCache(true); // Allow all if cannot fetch
            }
        });
    }
    
    // private static String fetchRobotsTxt(String domain) throws IOException {
    //     String robotsUrl = domain + "/robots.txt";
    //     return Jsoup.connect(robotsUrl)
    //               .userAgent(USER_AGENT)
    //               .ignoreContentType(true)
    //               .timeout(5000)
    //               .execute()
    //               .body();
    // }

    private static String fetchRobotsTxt(String domain) throws IOException {
        String robotsUrl = domain + "/robots.txt";
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return Jsoup.connect(robotsUrl)
                          .userAgent(USER_AGENT)
                          .ignoreContentType(true)
                          .timeout(3000) // Reduced from 5000
                          .execute()
                          .body();
            } catch (IOException e) {
                if (i == maxRetries - 1) throw e;
                System.out.println("Retrying robots.txt fetch for " + robotsUrl + " (attempt " + (i + 1) + ")");
                try {
                    Thread.sleep(1000); // Wait 1 second before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new IOException("Failed to fetch robots.txt after " + maxRetries + " attempts");
    }
    
    private static String getDomainKey(String url) throws Exception {
        URL u = new URL(url);
        return u.getProtocol() + "://" + u.getHost() + 
               (u.getPort() != -1 ? ":" + u.getPort() : "");
    }
    
    private static String getPath(String url) throws Exception {
        URL u = new URL(url);
        return u.getPath();
    }
    
    static class RobotsTxtCache {
        private final Map<String, Boolean> rules = new HashMap<>();
        private long crawlDelay = DEFAULT_DELAY;
        private boolean defaultAllow;
        
        public RobotsTxtCache(boolean defaultAllow) {
            this.defaultAllow = defaultAllow;
        }
        
        public RobotsTxtCache(String robotsTxt) {
            parseRobotsTxt(robotsTxt);
        }
        
        private void parseRobotsTxt(String robotsTxt) {
            String[] lines = robotsTxt.split("\n");
            String currentUserAgent = null;
            
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                if (line.toLowerCase().startsWith("user-agent:")) {
                    currentUserAgent = line.substring(11).trim();
                } else if (currentUserAgent != null && 
                          (currentUserAgent.equals("*") || currentUserAgent.equals(USER_AGENT))) {
                    if (line.toLowerCase().startsWith("disallow:")) {
                        String path = line.substring(9).trim();
                        rules.put(path, false);
                    } else if (line.toLowerCase().startsWith("allow:")) {
                        String path = line.substring(6).trim();
                        rules.put(path, true);
                    } else if (line.toLowerCase().startsWith("crawl-delay:")) {
                        try {
                            crawlDelay = Long.parseLong(line.substring(12).trim()) * 1000;
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }
        
        public boolean isAllowed(String path) {
            if (defaultAllow) return true;
            
            // Find most specific matching rule
            String bestMatch = "";
            for (String rulePath : rules.keySet()) {
                if (path.startsWith(rulePath) && rulePath.length() > bestMatch.length()) {
                    bestMatch = rulePath;
                }
            }
            
            return rules.getOrDefault(bestMatch, true);
        }
        
        public long getCrawlDelay() {
            return crawlDelay;
        }
    }
}