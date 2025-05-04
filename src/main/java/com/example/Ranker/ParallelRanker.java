package com.example.Ranker;

import com.example.QueryProcessor.QDocument;
import com.example.QueryProcessor.TermStats;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ParallelRanker {
    private final double titleBoost = 5.0;
    private final double alpha = 0.7;
    private final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private final int PARALLEL_THRESHOLD = 2000;

    public List<RankedDocument> rank(QueryInput input) {
        int docCount = input.getCandidateDocuments().size();
        
        if (docCount < PARALLEL_THRESHOLD) {
            return rankSequentially(input);
        } else {
            return rankInParallel(input);
        }
    }

    private List<RankedDocument> rankSequentially(QueryInput input) {
        return input.getCandidateDocuments().entrySet().stream()
                .map(entry -> createRankedDocument(entry.getKey(), entry.getValue(), input))
                .sorted(Comparator.comparingDouble(RankedDocument::getScore).reversed())
                .collect(Collectors.toList());
    }

    private List<RankedDocument> rankInParallel(QueryInput input) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Callable<List<RankedDocument>>> tasks = new ArrayList<>();
        
        // Split documents into chunks for each thread
        List<Map.Entry<String, QDocument>> entries = new ArrayList<>(input.getCandidateDocuments().entrySet());
        int chunkSize = (int) Math.ceil((double) entries.size() / THREAD_POOL_SIZE);
        
        for (int i = 0; i < entries.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, entries.size());
            List<Map.Entry<String, QDocument>> chunk = entries.subList(i, end);
            tasks.add(() -> processChunk(chunk, input));
        }

        // Execute tasks and collect results
        List<RankedDocument> results = new ArrayList<>();
        try {
            List<Future<List<RankedDocument>>> futures = executor.invokeAll(tasks);
            for (Future<List<RankedDocument>> future : futures) {
                results.addAll(future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error during parallel ranking: " + e.getMessage());
            // Fallback to sequential if parallel fails
            return rankSequentially(input);
        } finally {
            executor.shutdown();
        }

        // Sort final results
        return results.stream()
                .sorted(Comparator.comparingDouble(RankedDocument::getScore).reversed())
                .collect(Collectors.toList());
    }

    private List<RankedDocument> processChunk(List<Map.Entry<String, QDocument>> chunk, QueryInput input) {
        return chunk.stream()
                .map(entry -> createRankedDocument(entry.getKey(), entry.getValue(), input))
                .collect(Collectors.toList());
    }

    private RankedDocument createRankedDocument(String docId, QDocument doc, QueryInput input) {
        double relevance = computeRelevanceScore(input, doc);
        double popularity = doc != null && doc.getMetadata() != null ? doc.getMetadata().getPopularity() : 0.0;
        double score = alpha * relevance + (1 - alpha) * popularity;
        String URL = doc.getMetadata().getUrl();
        String snippet = doc.getMetadata().getSnippet();
        String title = doc.getMetadata().getTitle();
        return new RankedDocument(docId, score, relevance, popularity,URL,snippet,title);
    }
    
    private double computeRelevanceScore(QueryInput input, QDocument doc) {
        double score = 0.0;
        Map<String, TermStats> termStatsMap = doc.getTermStats();
        Map<String, Double> termIdfMap = input.getGlobalStats().getTermIDF();
    
        for (String term : input.getQueryTerms()) {
            TermStats stats = termStatsMap != null ? termStatsMap.get(term) : null;
            if (stats != null) {
                double tf = stats.getTf();
    
                Double idfObj = termIdfMap != null ? termIdfMap.get(term) : null;
                double idf = idfObj != null ? idfObj : 0.0;
    
                double boost = stats.isInTitle() ? titleBoost : 1.0;
                score += tf * idf * boost;
            }
        }
        return score;
    }
    
    
}