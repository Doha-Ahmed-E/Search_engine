package com.example.Popularity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;

public class PageRank {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double EPSILON = 1e-6;
    private static final int MAX_ITERATIONS = 100;
    private static final int BATCH_SIZE = 1000; // Batch size for MongoDB updates

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "finalV2DB";
    private static final String COLLECTION_NAME = "pages";

    public static Map<String, Double> computePageRank(Map<String, List<String>> graph) {
        final int N = graph.size();
        if (N == 0) return new HashMap<>();

        // Precompute incoming links map for faster access
        Map<String, List<String>> incomingLinksMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : graph.entrySet()) {
            String source = entry.getKey();
            for (String target : entry.getValue()) {
                incomingLinksMap.computeIfAbsent(target, k -> new java.util.ArrayList<>()).add(source);
            }
        }

        // Initialize PageRank values
        final double initialRank = 1.0 / N;
        final Map<String, Double>[] currentPr = new Map[]{new ConcurrentHashMap<>()};
        graph.keySet().parallelStream().forEach(page -> currentPr[0].put(page, initialRank));

        // Precompute dangling nodes
        List<String> danglingNodes = graph.entrySet().parallelStream()
                .filter(e -> e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .toList();

        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            Map<String, Double> newPr = new ConcurrentHashMap<>();

            // Compute dangling mass in parallel
            double danglingMass = danglingNodes.parallelStream()
                    .mapToDouble(node -> currentPr[0].get(node))
                    .sum();

            double teleport = (1 - DAMPING_FACTOR) / N;
            double danglingContribution = DAMPING_FACTOR * danglingMass / N;

            // Parallel processing of pages
            graph.keySet().parallelStream().forEach(page -> {
                double incomingSum = 0.0;
                List<String> incomingLinks = incomingLinksMap.get(page);
                if (incomingLinks != null) {
                    for (String source : incomingLinks) {
                        int outDegree = graph.get(source).size();
                        if (outDegree > 0) {
                            incomingSum += currentPr[0].get(source) / outDegree;
                        }
                    }
                }
                newPr.put(page, teleport + DAMPING_FACTOR * incomingSum + danglingContribution);
            });

            // Normalize
            double sum = newPr.values().parallelStream().mapToDouble(Double::doubleValue).sum();
            newPr.replaceAll((k, v) -> v / sum);

            System.out.printf("Iter %d: Sum=%.6f Dangling=%.6f%n", iter, sum, danglingMass);

            if (converged(currentPr[0], newPr)) {
                System.out.println("Converged after " + (iter + 1) + " iterations");
                currentPr[0] = newPr;
                break;
            }
            currentPr[0] = newPr;
        }
        return currentPr[0];
    }

    private static boolean converged(Map<String, Double> oldPr, Map<String, Double> newPr) {
        return oldPr.entrySet().parallelStream()
                .allMatch(entry -> Math.abs(entry.getValue() - newPr.get(entry.getKey())) <= EPSILON);
    }

    public static Map<String, List<String>> buildGraphFromDataBase() {
        Map<String, List<String>> graph = new HashMap<>();

        try (MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            System.out.println("Total documents in collection: " + collection.countDocuments());
            
            // Use batch processing for better performance
            int batchCount = 0;
            for (Document doc : collection.find().batchSize(1000)) {
                String url = doc.getString("url");
                List<String> outgoingLinks = doc.getList("outgoingLinks", String.class);
                
                if (outgoingLinks == null) {
                    outgoingLinks = List.of(); // Ensure we don't have null lists
                }
                
                graph.put(url, outgoingLinks);
                
                if (++batchCount % 1000 == 0) {
                    System.out.println("Processed " + batchCount + " documents");
                }
            }
            System.out.println("Finished processing. Total documents: " + batchCount);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Graph contains " + graph.size() + " nodes");
        return graph;
    }

    public static void matchDocumentWithPageRank(Map<String, Double> ranks) {
        try (MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            // Use bulk writes for better performance
            List<WriteModel<Document>> bulkOperations = new java.util.ArrayList<>();
            int operationCount = 0;

            for (Map.Entry<String, Double> entry : ranks.entrySet()) {
                String url = entry.getKey();
                double pageRank = entry.getValue();

                bulkOperations.add(
                    new UpdateOneModel<>(
                        new Document("url", url),
                        Updates.combine(
                            Updates.set("popularity", pageRank)
                        ),
                        new UpdateOptions().upsert(false)
                    )
                );

                if (++operationCount % BATCH_SIZE == 0) {
                    collection.bulkWrite(bulkOperations);
                    bulkOperations.clear();
                    System.out.println("Processed " + operationCount + " updates");
                }
            }

            // Process remaining operations
            if (!bulkOperations.isEmpty()) {
                collection.bulkWrite(bulkOperations);
                System.out.println("Processed remaining " + bulkOperations.size() + " updates");
            }

            System.out.println("Updated " + operationCount + " documents with PageRank values.");
        } catch (Exception e) {
            System.err.println("ERROR updating MongoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        
        System.out.println("Building graph from database...");
        Map<String, List<String>> graph = buildGraphFromDataBase();

        System.out.println("Computing PageRank...");
        Map<String, Double> ranks = computePageRank(graph);

        System.out.println("\nPageRank Results Summary:");
        double sum = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
        System.out.printf("Sum of all PageRanks: %.6f%n", sum);
        System.out.println("Total pages ranked: " + ranks.size());

        System.out.println("Updating documents with PageRank values...");
        matchDocumentWithPageRank(ranks);

        long endTime = System.currentTimeMillis();
        System.out.printf("Total execution time: %.2f seconds%n", (endTime - startTime) / 1000.0);
    }
}