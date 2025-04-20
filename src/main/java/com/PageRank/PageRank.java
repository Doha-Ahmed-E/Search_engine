package com.PageRank;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageRank {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double EPSILON = 1e-6;
    private static final int MAX_ITERATIONS = 100;

    public static Map<String, Double> computePageRank(Map<String, List<String>> graph) {
        Map<String, Double> pageRank = new HashMap<>();
        int totalPages = graph.size();
        double initialRank = 1.0 / totalPages;

        for (String page : graph.keySet()) {
            pageRank.put(page, initialRank);
        }

        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            Map<String, Double> newPageRank = new HashMap<>();
            double danglingRank = 0.0;

            for (String page : graph.keySet()) {
                if (graph.get(page).isEmpty()) {
                    danglingRank += pageRank.get(page);
                }
            }

            danglingRank /= totalPages;

            for (String page : graph.keySet()) {
                double sum = 0.0;
                for (String incoming : graph.keySet()) {
                    if (graph.get(incoming).contains(page)) {
                        sum += pageRank.get(incoming) / graph.get(incoming).size();
                    }
                }
                newPageRank.put(page, (1 - DAMPING_FACTOR)/totalPages + 
                    DAMPING_FACTOR * (sum + danglingRank));
            }

            boolean converged = true;
            for (String page : pageRank.keySet()) {
                if (Math.abs(newPageRank.get(page) - pageRank.get(page)) > EPSILON) {
                    converged = false;
                    break;
                }
            }

            pageRank = newPageRank;
            if (converged) break;
        }

        return pageRank;
    }

    public static void main(String[] args) {
        Map<String, List<String>> graph = new HashMap<>();
        graph.put("A", Arrays.asList("B", "C"));
        graph.put("B", Arrays.asList("C"));
        graph.put("C", Arrays.asList("A"));

        Map<String, Double> ranks = computePageRank(graph);
        ranks.forEach((page, rank) -> System.out.println(page + ": " + rank));
        
        double sum = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
        System.out.println("Sum: " + sum);
    }
}