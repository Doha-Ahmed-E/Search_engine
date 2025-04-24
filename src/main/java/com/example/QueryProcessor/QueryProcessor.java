package com.example.QueryProcessor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.example.DatabaseConnection;
import com.example.Indexer.Stemmer;
import com.example.Indexer.StopWords;
import com.example.Indexer.Tokenizer;
import com.example.Ranker.QueryInput;
import org.bson.Document;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

public class QueryProcessor {
    private final MongoCollection<Document> wordIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;
    private final ObjectMapper objectMapper;

    public QueryProcessor() {
        wordIndexCollection = DatabaseConnection.getDatabase().getCollection("word_index");
        documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        tokenizer = new Tokenizer();
        stopWords = new StopWords();
        stemmer = new Stemmer();
        objectMapper = new ObjectMapper();
    }

    public QueryInput processQuery(String queryString, int maxResults) {
        try {
            if (queryString.startsWith("\"") && queryString.endsWith("\"")) {
                return processPhraseQuery(queryString, maxResults);
            }
            
            List<String> queryTerms = preprocessQuery(queryString);
            QueryInput queryInput = new QueryInput();
            if (queryTerms.isEmpty()) return queryInput;

            queryInput.setQueryTerms(queryTerms);
            Map<String, Integer> docsContainingTerm = new HashMap<>();
            Map<String, Double> termIDF = new HashMap<>();
            GlobalStats globalStats = new GlobalStats();
            Map<String, QDocument> candidateDocs = collectDocumentInfo(queryTerms, docsContainingTerm, termIDF, globalStats);

            queryInput.setCandidateDocuments(candidateDocs);
            queryInput.setGlobalStats(globalStats);
            return queryInput;
        } catch (Exception e) {
            e.printStackTrace();
            return new QueryInput();
        }
    }

    private QueryInput processPhraseQuery(String queryString, int maxResults) {
        QueryInput queryInput = new QueryInput();
        String phrase = queryString.substring(1, queryString.length() - 1).trim();
        if (phrase.isEmpty()) return queryInput;

        List<String> phraseTerms = preprocessQuery(phrase);
        if (phraseTerms.size() < 2) return queryInput;

        queryInput.setQueryTerms(phraseTerms);

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();
        GlobalStats globalStats = new GlobalStats();
        Map<String, QDocument> allTermDocs = collectDocumentInfo(phraseTerms, docsContainingTerm, termIDF, globalStats);

        Map<String, QDocument> phraseDocs = new HashMap<>();
        Set<String> docsWithMetadata = new HashSet<>();

        for (String docId : allTermDocs.keySet()) {
            if (hasExactPhrase(docId, phraseTerms)) {
                phraseDocs.put(docId, allTermDocs.get(docId));
                if (!docsWithMetadata.contains(docId)) {
                    setDocumentMetadata(allTermDocs.get(docId), getDocumentPosting(docId, phraseTerms.get(0)));
                    docsWithMetadata.add(docId);
                }
            }
        }

        docsContainingTerm.put(phrase, phraseDocs.size());
        termIDF.put(phrase, calculatePhraseIDF(phraseDocs.size(), globalStats.getTotalDocs()));

        queryInput.setCandidateDocuments(phraseDocs);
        queryInput.setGlobalStats(globalStats);
        return queryInput;
    }

    private boolean hasExactPhrase(String docId, List<String> phraseTerms) {
        Map<String, List<Integer>> termPositions = new HashMap<>();
        
        for (String term : phraseTerms) {
            Document wordDoc = wordIndexCollection.find(Filters.and(
                Filters.eq("word", term),
                Filters.eq("postings.doc_id", docId)
            )).first();
            
            if (wordDoc == null) return false;
            
            List<Document> postings = wordDoc.getList("postings", Document.class);
            for (Document posting : postings) {
                if (docId.equals(posting.getString("doc_id"))) {
                    termPositions.put(term, posting.getList("positions", Integer.class));
                    break;
                }
            }
        }

        if (termPositions.size() != phraseTerms.size()) return false;

        List<Integer> firstTermPositions = termPositions.get(phraseTerms.get(0));
        for (int pos : firstTermPositions) {
            boolean match = true;
            for (int i = 1; i < phraseTerms.size(); i++) {
                int expectedPos = pos + i;
                if (!termPositions.get(phraseTerms.get(i)).contains(expectedPos)) {
                    match = false;
                    break;
                }
            }
            if (match) return true;
        }
        return false;
    }

    private Document getDocumentPosting(String docId, String term) {
        Document wordDoc = wordIndexCollection.find(Filters.and(
            Filters.eq("word", term),
            Filters.eq("postings.doc_id", docId)
        )).first();
        
        if (wordDoc != null) {
            List<Document> postings = wordDoc.getList("postings", Document.class);
            for (Document posting : postings) {
                if (docId.equals(posting.getString("doc_id"))) {
                    return posting;
                }
            }
        }
        return null;
    }

    private double calculatePhraseIDF(int docsWithPhrase, int totalDocs) {
        return docsWithPhrase == 0 ? 0 : Math.log((double) totalDocs / docsWithPhrase);
    }

    private List<String> preprocessQuery(String queryString) {
        List<String> tokens = tokenizer.tokenize(queryString);
        tokens = stopWords.removeStopWords(tokens);
        List<String> stemmedTokens = new ArrayList<>();
        for (String token : tokens) {
            String stemmed = stemmer.stem(token);
            if (stemmed != null && !stemmed.isEmpty()) {
                stemmedTokens.add(stemmed);
            }
        }
        return stemmedTokens;
    }

    private Map<String, QDocument> collectDocumentInfo(List<String> queryTerms,
            Map<String, Integer> docsContainingTerm, Map<String, Double> termIDF,
            GlobalStats globalStats) {
        Map<String, QDocument> candidateDocs = new HashMap<>();
        Set<String> docsWithMetadata = new HashSet<>();

        for (String term : queryTerms) {
            Document wordDoc = wordIndexCollection.find(Filters.eq("word", term)).first();
            if (wordDoc == null) continue;

            docsContainingTerm.put(term, wordDoc.getInteger("doc_count"));
            termIDF.put(term, wordDoc.getDouble("idf"));

            List<Document> postings = wordDoc.getList("postings", Document.class);
            if (postings == null) continue;
            for (Document posting : postings) {
                String docId = posting.getString("doc_id");
                QDocument qDoc = processTermStatistics(candidateDocs, term, docId, posting);
                if (!docsWithMetadata.contains(docId)) {
                    setDocumentMetadata(qDoc, posting);
                    docsWithMetadata.add(docId);
                }
            }
        }

        globalStats.setTotalDocs((int) documentsCollection.countDocuments());
        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);
        return candidateDocs;
    }

    private QDocument processTermStatistics(Map<String, QDocument> candidateDocs, String term,
            String docId, Document posting) {
        QDocument qDoc = candidateDocs.computeIfAbsent(docId, k -> new QDocument());
        Map<String, TermStats> termInfoMap = qDoc.getTermStats();
        if (termInfoMap == null) {
            termInfoMap = new HashMap<>();
            qDoc.setTermStats(termInfoMap);
        }
        TermStats termStats = new TermStats(posting.getBoolean("in_title"));
        termStats.setTf(posting.getDouble("tf"));
        termStats.setImportanceScore(posting.getDouble("importance_score"));
        termInfoMap.put(term, termStats);
        return qDoc;
    }

    private void setDocumentMetadata(QDocument qDoc, Document posting) {
        if (posting == null) return;
        Metadata metadata = new Metadata();
        metadata.setUrl(posting.getString("url"));
        metadata.setPopularity(0.5);
        metadata.setLength(posting.getInteger("length", 0));
        Long timestamp = posting.getLong("timestamp");
        if (timestamp != null) {
            metadata.setPublishDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp)));
        } else {
            metadata.setPublishDate("");
        }
        qDoc.setMetadata(metadata);
    }

    public void saveQueryResponseToFile(String query, String filename) {
        try {
            objectMapper.writeValue(new File(filename), processQuery(query, 10));
            System.out.println("Query response saved to " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            QueryProcessor processor = new QueryProcessor();
            // String query = args.length > 0 ? args[0] : "stack and Java # programming maybe sql";
            String query = "\"account another\"";
            
            System.out.println("\nProcessing query: \"" + query + "\"");
            long startTime = System.currentTimeMillis();
            QueryInput result = processor.processQuery(query, 10);
            long endTime = System.currentTimeMillis();
            
            System.out.println("Query processing time: " + (endTime - startTime) + "ms\n");
            printQueryResults(result);
            
            String filename = "query_result_" + System.currentTimeMillis() + ".json";
            processor.saveQueryResponseToFile(query, filename);
            System.out.println("\nDetailed results saved to: " + filename);
        } catch (Exception e) {
            System.err.println("Error processing query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printQueryResults(QueryInput result) {
        System.out.println("Query terms: " + result.getQueryTerms());
        GlobalStats stats = result.getGlobalStats();
        System.out.println("\nGlobal statistics:");
        System.out.println("  Total documents: " + stats.getTotalDocs());
        System.out.println("  Documents containing terms:");
        
        if (stats.getDocsContainingTerm() != null) {
            stats.getDocsContainingTerm().forEach((k, v) -> 
                System.out.println("    " + k + ": " + v + " docs"));
        }
        
        System.out.println("  Term IDF values:");
        if (stats.getTermIDF() != null) {
            stats.getTermIDF().forEach((k, v) -> System.out.println("    " + k + ": " + v));
        }
        
        Map<String, QDocument> docs = result.getCandidateDocuments();
        System.out.println("\nFound " + (docs != null ? docs.size() : 0) + " candidate documents:");
        
        if (docs != null && !docs.isEmpty()) {
            int count = 0;
            for (Map.Entry<String, QDocument> entry : docs.entrySet()) {
                if (++count > 5 && docs.size() > 5) {
                    System.out.println("\n...and " + (docs.size() - 5) + " more documents");
                    break;
                }
                printDocumentDetails(count, entry);
            }
        } else {
            System.out.println("No matching documents found.");
        }
    }

    private static void printDocumentDetails(int count, Map.Entry<String, QDocument> entry) {
        System.out.println("\n" + count + ". Document: " + entry.getKey());
        QDocument doc = entry.getValue();
        System.out.println("   URL: " + doc.getMetadata().getUrl());
        
        Metadata metadata = doc.getMetadata();
        if (metadata != null) {
            System.out.println("   Popularity: " + metadata.getPopularity());
            System.out.println("   Length: " + metadata.getLength());
            System.out.println("   Publish date: " + metadata.getPublishDate());
        }
        
        if (doc.getTermStats() != null && !doc.getTermStats().isEmpty()) {
            System.out.println("   Term statistics:");
            doc.getTermStats().forEach((k, v) -> System.out.println(
                "     " + k + ": tf=" + v.getTf() + ", inTitle=" + 
                v.isInTitle() + ", importance=" + v.getImportanceScore()));
        }
    }
}