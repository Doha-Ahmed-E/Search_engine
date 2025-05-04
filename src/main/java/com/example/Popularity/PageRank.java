package com.example.Popularity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



public class PageRank
{
    private static final double DAMPING_FACTOR = 0.85;
    private static final double EPSILON = 1e-6;
    private static final int MAX_ITERATIONS = 100;



    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "newDb";
    private static final String COLLECTION_NAME = "pages";

    public static Map<String, Double> computePageRank(Map<String, List<String>> graph)
    {
        final int N = graph.size();
        final Map<String, Double> pr = new HashMap<>();
        final double initialRank = 1.0 / N;

        for (String page : graph.keySet())
        {
            pr.put(page, initialRank);
        }

        Map<String, Double> currentPr = new HashMap<>(pr);

        for (int iter = 0; iter < MAX_ITERATIONS; iter++)
        {
            Map<String, Double> newPr = new HashMap<>();

            final Map<String, Double> finalPr = currentPr;
            double danglingMass = graph.entrySet().stream().filter(e -> e.getValue().isEmpty())
                    .mapToDouble(e -> finalPr.get(e.getKey())).sum();

            double teleport = (1 - DAMPING_FACTOR) / N;
            double danglingContribution = DAMPING_FACTOR * danglingMass / N;

            for (String page : graph.keySet())
            {
                double incomingSum = 0.0;
                for (Map.Entry<String, List<String>> entry : graph.entrySet())
                {
                    if (entry.getValue().contains(page))
                    {
                        incomingSum += currentPr.get(entry.getKey()) / entry.getValue().size();
                    }
                }
                newPr.put(page, teleport + DAMPING_FACTOR * incomingSum + danglingContribution);
            }

            double sum = newPr.values().stream().mapToDouble(Double::doubleValue).sum();
            for (String page : newPr.keySet())
            {
                newPr.put(page, newPr.get(page) / sum);
            }

            sum = newPr.values().stream().mapToDouble(Double::doubleValue).sum();
            System.out.printf("Iter %d: Sum=%.6f Dangling=%.6f%n", iter, sum, danglingMass);

            if (converged(currentPr, newPr))
            {
                System.out.println("Converged after " + (iter + 1) + " iterations");
                currentPr = newPr;
                break;
            }
            currentPr = newPr;
        }
        return currentPr;
    }

    private static boolean converged(Map<String, Double> oldPr, Map<String, Double> newPr)
    {
        for (String page : oldPr.keySet())
        {
            if (Math.abs(oldPr.get(page) - newPr.get(page)) > EPSILON)
            {
                return false;
            }
        }
        return true;
    }

    public static Map<String, List<String>> buildGraphFromDataBase()
    {
        Map<String, List<String>> graph = new HashMap<>();

        try (MongoClient mongoClient = MongoClients.create(MONGO_URI))
        {
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);


            System.out.println("Total documents in collection: " + collection.countDocuments());
            int count = 0;
            for (Document doc : collection.find())
            {
                String url = doc.getString("url");
                List<String> outgoingLinks = doc.getList("outgoingLinks", String.class);

                System.out.println("Processing URL: " + url);
                System.out.println("Found " + (outgoingLinks != null ? outgoingLinks.size() : 0)
                        + " outgoing links " + count);


                List<String> last = graph.put(url, outgoingLinks);
                if (last == null)
                    count++;

            }
            System.out.println("=============================");
            System.out.println(count);
            System.out.println("=============================");

        }
        catch (Exception e)
        {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Graph contains " + graph.size() + " nodes");
        return graph;
    }

    public static void matchDocumentWithPageRank(Map<String, Double> ranks)
    {
        try (MongoClient mongoClient = MongoClients.create(MONGO_URI))
        {
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            for (Document doc : collection.find())
            {
                String url = doc.getString("url");
                double pageRank;

                if (ranks.containsKey(url))
                {
                    pageRank = ranks.get(url);
                }
                else
                {
                    System.err.println("WARNING: URL not found in PageRank results: " + url);
                    pageRank = 0.0;
                }

                collection.updateOne(new Document("url", url),
                        new Document("$set", new Document("popularity", pageRank)));

            }
            System.out.println("Updated all documents with PageRank values.");
        }
        catch (Exception e)
        {
            System.err.println("ERROR updating MongoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        Map<String, List<String>> graph = buildGraphFromDataBase();

        Map<String, Double> ranks = computePageRank(graph);


        System.out.println("\nPageRank Results:");

        int count = 0;
        for (Map.Entry<String, Double> entry : ranks.entrySet())
        {
            String url = entry.getKey();
            Double score = entry.getValue();
            System.out.printf("%s: %.6f%n", url, score);
            count++;
        }

        double sum = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
        System.out.printf("\nSum of all PageRanks: %.6f%n", sum);
        System.out.println("the count is " + count);

        matchDocumentWithPageRank(ranks);
    }
}
