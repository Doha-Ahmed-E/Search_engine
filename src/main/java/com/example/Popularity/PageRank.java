package com.example.Popularity;

import com.example.DatabaseConnection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PageRank {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double EPSILON = 1e-6;
    private static final int MAX_ITERATIONS = 100;
    private static final int BATCH_SIZE = 1000;

    private final MongoCollection<Document> pagesCollection;
    private final MongoCollection<Document> documentsCollection;

    public PageRank() {
        MongoDatabase database = DatabaseConnection.getDatabase();
        this.pagesCollection = database.getCollection("pages");
        this.documentsCollection = database.getCollection("documents");
    }

    public void computeAndStorePageRanks() {
        try {
            System.out.println("Starting PageRank computation...");
            
            // Step 1: Build the web graph from database
            Map<String, List<String>> graph = buildGraphFromDatabase();
            
            if (graph.isEmpty()) {
                System.out.println("No documents found to compute PageRank");
                return;
            }

            // Step 2: Compute PageRank scores
            Map<String, Double> ranks = computePageRank(graph);

            // Step 3: Update pages with PageRank scores
            updatePagesWithPageRank(ranks);

            System.out.println("PageRank computation completed successfully");
        } catch (Exception e) {
            System.err.println("Error in PageRank computation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Map<String, List<String>> buildGraphFromDatabase() {
        Map<String, List<String>> graph = new ConcurrentHashMap<>();
        long totalDocuments = pagesCollection.countDocuments();
        System.out.println("Building graph from " + totalDocuments + " documents");

        // Process in batches to handle large collections
        int processed = 0;
        for (Document doc : pagesCollection.find()) {
            try {
                String url = doc.getString("url");
                List<String> outgoingLinks = doc.getList("outgoingLinks", String.class);

                if (url != null && !url.isEmpty()) {
                    // Normalize URLs and filter out invalid ones
                    List<String> validLinks = outgoingLinks != null ? 
                        outgoingLinks.stream()
                            .filter(link -> link != null && !link.isEmpty())
                            .collect(Collectors.toList()) : 
                        Collections.emptyList();

                    graph.put(url, validLinks);
                    processed++;

                    if (processed % BATCH_SIZE == 0) {
                        System.out.println("Processed " + processed + " documents");
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing document: " + e.getMessage());
            }
        }

        System.out.println("Graph construction complete. Total nodes: " + graph.size());
        return graph;
    }

    private Map<String, Double> computePageRank(Map<String, List<String>> graph) {
        final int N = graph.size();
        System.out.println("Computing PageRank for " + N + " pages");
    
        // Initialize PageRank values
        Map<String, Double>[] currentPrHolder = new Map[]{new ConcurrentHashMap<>()}; // Using array to hold mutable reference
        double initialRank = 1.0 / N;
        graph.keySet().forEach(page -> currentPrHolder[0].put(page, initialRank));
    
        // Precompute incoming links for faster access
        Map<String, List<String>> incomingLinks = precomputeIncomingLinks(graph);
    
        // Iterative PageRank computation
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            Map<String, Double> newPr = new ConcurrentHashMap<>();
    
            // Calculate dangling nodes contribution
            double danglingMass = graph.entrySet().parallelStream()
                .filter(e -> e.getValue().isEmpty())
                .mapToDouble(e -> currentPrHolder[0].get(e.getKey()))
                .sum();
    
            final double teleport = (1 - DAMPING_FACTOR) / N;
            final double danglingContribution = DAMPING_FACTOR * danglingMass / N;
    
            // Compute new PageRank for each page
            graph.keySet().parallelStream().forEach(page -> {
                double incomingSum = incomingLinks.getOrDefault(page, Collections.emptyList())
                    .parallelStream()
                    .mapToDouble(incomingPage -> {
                        int outDegree = graph.get(incomingPage).size();
                        return outDegree > 0 ? currentPrHolder[0].get(incomingPage) / outDegree : 0;
                    })
                    .sum();
    
                double newRank = teleport + DAMPING_FACTOR * incomingSum + danglingContribution;
                newPr.put(page, newRank);
            });
    
            // Normalize to ensure sum = 1 (handles floating point errors)
            double sum = newPr.values().parallelStream().mapToDouble(Double::doubleValue).sum();
            newPr.replaceAll((k, v) -> v / sum);
    
            System.out.printf("Iteration %d: Sum=%.6f Dangling=%.6f%n", iter + 1, sum, danglingMass);
    
            // Check for convergence
            if (converged(currentPrHolder[0], newPr)) {
                System.out.println("Converged after " + (iter + 1) + " iterations");
                currentPrHolder[0] = newPr;
                break;
            }
            currentPrHolder[0] = newPr;
        }
    
        return currentPrHolder[0];
    }

    private Map<String, List<String>> precomputeIncomingLinks(Map<String, List<String>> graph) {
        Map<String, List<String>> incomingLinks = new ConcurrentHashMap<>();
        
        graph.forEach((page, outgoing) -> {
            outgoing.forEach(link -> {
                incomingLinks.computeIfAbsent(link, k -> new ArrayList<>()).add(page);
            });
        });
        
        return incomingLinks;
    }

    private boolean converged(Map<String, Double> oldPr, Map<String, Double> newPr) {
        return oldPr.keySet().parallelStream()
            .allMatch(page -> Math.abs(oldPr.get(page) - newPr.get(page)) <= EPSILON);
    }

    private void updatePagesWithPageRank(Map<String, Double> ranks) {
        System.out.println("Updating pages with PageRank values");
        int updatedCount = 0;
        
        // Process in batches for better performance
        List<WriteModel<Document>> batchUpdates = new ArrayList<>();
        
        for (Document page : pagesCollection.find()) {
            try {
                String url = page.getString("url");
                if (url != null && !url.isEmpty() && ranks.containsKey(url)) {
                    double pageRank = ranks.get(url);
                    
                    batchUpdates.add(new UpdateOneModel<>(
                        new Document("url", url),
                        new Document("$set", new Document("popularity", pageRank))
                    ));
                    
                    if (batchUpdates.size() >= BATCH_SIZE) {
                        pagesCollection.bulkWrite(batchUpdates);
                        updatedCount += batchUpdates.size();
                        batchUpdates.clear();
                        System.out.println("Updated " + updatedCount + " pages");
                    }
                }
            } catch (Exception e) {
                System.err.println("Error updating page: " + e.getMessage());
            }
        }
        
        // Process remaining updates
        if (!batchUpdates.isEmpty()) {
            pagesCollection.bulkWrite(batchUpdates);
            updatedCount += batchUpdates.size();
            System.out.println("Updated final batch of " + batchUpdates.size() + " pages");
        }
        
        System.out.println("Successfully updated " + updatedCount + " pages with PageRank values");
    }

    public static void main(String[] args) {
        try {
            PageRank pageRank = new PageRank();
            pageRank.computeAndStorePageRanks();
        } catch (Exception e) {
            System.err.println("Error in PageRank main execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            DatabaseConnection.closeConnection();
        }
    }
}