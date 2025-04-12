package com.example.Crawler;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

//Purpose: Manages the priority queue of URLs to crawl.
//Thread-safe PriorityBlockingQueue implementation
//ReentrantLock for concurrent access control (optional, since PriorityBlockingQueue is thread-safe)
//WebPage inner class with URL and priority score
//Scores determine crawl order (higher scores first)

public class PagePriority {
    private final PriorityBlockingQueue<WebPage> queue = new PriorityBlockingQueue<>();
    
    public void addPage(String url, int linkScore) {
        queue.add(new WebPage(url, linkScore));
    }

    public WebPage getNextPage() throws InterruptedException {
        return queue.take(); // Blocks until a page is available
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public static class WebPage implements Comparable<WebPage> {
        final String url;
        final int linkScore;

        public WebPage(String url, int linkScore) {
            this.url = url;
            this.linkScore = linkScore;
        }

        @Override
        public int compareTo(WebPage other) {
            return Integer.compare(this.linkScore, other.linkScore); // Higher score first
        }
    }
}