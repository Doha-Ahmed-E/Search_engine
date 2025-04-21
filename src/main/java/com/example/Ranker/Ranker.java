package com.example.Ranker;

import com.example.QueryProcessor.QDocument;
import com.example.QueryProcessor.TermStats;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class Ranker
{
    private final double titleBoost = 2.0;
    private final double alpha = 0.7;

    public List<RankedDocument> rank(QueryInput input)
    {
        return input.getCandidateDocuments().entrySet().stream().map(entry -> {
            String docId = entry.getKey();
            QDocument doc = entry.getValue();
            double relevance = computeRelevanceScore(input, doc);
            double popularity = doc.getMetadata().getPopularity();
            double score = alpha * relevance + (1 - alpha) * popularity;
            return new RankedDocument(docId, score, relevance, popularity);
        }).sorted(Comparator.comparingDouble(RankedDocument::getScore).reversed())
                .collect(Collectors.toList());
    }

    private double computeRelevanceScore(QueryInput input, QDocument doc)
    {
        double score = 0.0;
        for (String term : input.getQueryTerms())
        {
            TermStats stats = doc.getTermStats().get(term);
            if (stats != null)
            {
                double tf = stats.getTf();
                double idf =  input.getGlobalStats().getTermIDF().getOrDefault(term, 0.0);
                double boost = stats.isInTitle() ? titleBoost : 1.0;
                score += tf * idf * boost;
            }
        }
        return score;
    }


    public static void main(String[] args) throws Exception
    {
        // 1. Parse JSON input
        ObjectMapper mapper = new ObjectMapper();
        QueryInput input = mapper.readValue(new File("src/main/resources/query_results.json"),
                QueryInput.class);

        // 2. Rank documents
        Ranker ranker = new Ranker();
        List<RankedDocument> results = ranker.rank(input);

        // 3. Print results
        System.out.println("Ranked Results:");
        results.forEach(
                doc -> System.out.printf("%s: score=%.3f (relevance=%.3f, popularity=%.1f)%n",
                        doc.getDocId(), doc.getScore(), doc.getRelevance(), doc.getPopularity()));
    }
}


class RankedDocument
{
    private final String docId;
    private final double score;
    private final double relevance;
    private final double popularity;

    // Constructor and getters
    public RankedDocument(String docId, double score, double relevance, double popularity)
    {
        this.docId = docId;
        this.score = score;
        this.relevance = relevance;
        this.popularity = popularity;
    }

    public String getDocId()
    {
        return docId;
    }

    public double getScore()
    {
        return score;
    }

    public double getRelevance()
    {
        return relevance;
    }

    public double getPopularity()
    {
        return popularity;
    }
}
