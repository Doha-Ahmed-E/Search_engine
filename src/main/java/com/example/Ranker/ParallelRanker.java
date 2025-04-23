package com.example.Ranker;

import com.example.QueryProcessor.QDocument;
import com.example.QueryProcessor.TermStats;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ParallelRanker {
    private final double titleBoost = 2.0;
    private final double alpha = 0.7;
    private final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private final int PARALLEL_THRESHOLD = 100;

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
        double popularity = doc.getMetadata().getPopularity();
        double score = alpha * relevance + (1 - alpha) * popularity;
        return new RankedDocument(docId, score, relevance, popularity);
    }

    private double computeRelevanceScore(QueryInput input, QDocument doc) {
        double score = 0.0;
        for (String term : input.getQueryTerms()) {
            TermStats stats = doc.getTermStats().get(term);
            if (stats != null) {
                double tf = stats.getTf();
                double idf = input.getGlobalStats().getTermIDF().getOrDefault(term, 0.0);
                double boost = stats.isInTitle() ? titleBoost : 1.0;
                score += tf * idf * boost;
            }
        }
        return score;
    }

    public static void main(String[] args) throws Exception {
        // 1. Parse JSON input
        ObjectMapper mapper = new ObjectMapper();
        QueryInput input = mapper.readValue(new File("src/main/resources/query_results.json"),
                QueryInput.class);

        // 2. Rank documents
        ParallelRanker ranker = new ParallelRanker();
        long startTime = System.currentTimeMillis();
        List<RankedDocument> results = ranker.rank(input);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Operation took " + duration + " milliseconds");

        // 3. Print results
        System.out.println("Ranked Results:");
        results.forEach(
                doc -> System.out.printf("%s: score=%.3f (relevance=%.3f, popularity=%.1f)%n",
                        doc.getDocId(), doc.getScore(), doc.getRelevance(), doc.getPopularity()));
    }
}