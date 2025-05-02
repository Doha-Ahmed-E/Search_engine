package com.example.QueryProcessor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.example.DatabaseConnection;
import com.example.Indexer.Stemmer;
import com.example.Indexer.StopWords;
import com.example.Indexer.Tokenizer;
import com.example.Ranker.QueryInput;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.springframework.stereotype.Component;
import com.example.Ranker.RankedDocument;
import com.example.Ranker.ParallelRanker;
@Component
public class QueryProcessor {
    private final MongoCollection<Document> wordIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;
    private final ObjectMapper objectMapper;

    public QueryProcessor() {
        this.wordIndexCollection = DatabaseConnection.getDatabase().getCollection("inverted_index");
        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();
        this.objectMapper = new ObjectMapper();
    }

    public List<RankedDocument> processQuery(String queryString, int maxResults) {
        try {
            queryString = queryString.trim();
            QueryInput queryInput = new QueryInput();
            
            if (isPhraseLogicalQuery(queryString)) {
                queryInput= processPhraseLogicalQuery(queryString, maxResults);
            }
            
            if (queryString.startsWith("\"") && queryString.endsWith("\"")) {
                queryInput= processPhraseQuery(queryString, maxResults);
            }else{

                queryInput=processTermQuery(queryString, maxResults);
            }
            ParallelRanker ranker = new ParallelRanker();
            long startTime, endTime;
            startTime = System.currentTimeMillis();
            List<RankedDocument> results = ranker.rank(queryInput);
            
            endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            System.out.println("Operation took " + duration + " milliseconds");

            // 3. Print results
            System.out.println("Ranked Results:");
            results.forEach(
                    doc -> System.out.printf("%s: score=%.3f (relevance=%.3f, popularity=%.1f)%n",
                            doc.getDocId(), doc.getScore(), doc.getRelevance(), doc.getPopularity()));
            return results;
            
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    private QueryInput processTermQuery(String queryString, int maxResults) {
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
    }

    private boolean isPhraseLogicalQuery(String queryString) {
        boolean hasAnd = queryString.contains(" AND ");
        boolean hasOr = queryString.contains(" OR ");
        boolean hasNot = queryString.contains(" NOT ");
        
        int operatorCount = (hasAnd ? 1 : 0) + (hasOr ? 1 : 0) + (hasNot ? 1 : 0);
        if (operatorCount != 1) return false;
        
        String[] parts;
        if (hasAnd) {
            parts = queryString.split(" AND ");
        } else if (hasOr) {
            parts = queryString.split(" OR ");
        } else {
            parts = queryString.split(" NOT ");
        }
        
        return parts.length == 2 && 
               parts[0].trim().startsWith("\"") && parts[0].trim().endsWith("\"") &&
               parts[1].trim().startsWith("\"") && parts[1].trim().endsWith("\"");
    }

    private QueryInput processPhraseQuery(String queryString, int maxResults) {
        QueryInput queryInput = new QueryInput();
        String phrase = queryString.trim().substring(1, queryString.length() - 1).trim();
        if (phrase.isEmpty()) return queryInput;

        List<String> phraseTerms = preprocessQuery(phrase);
        if (phraseTerms.size() < 2) return processTermQuery(phrase, maxResults);
        
        String stemmedPhrase = String.join(" ", phraseTerms);
        List<String> queryTerms = new ArrayList<>();
        queryTerms.add(stemmedPhrase);
        queryInput.setQueryTerms(queryTerms);

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();
        GlobalStats globalStats = new GlobalStats();
        Map<String, QDocument> allTermDocs = collectDocumentInfo(phraseTerms, docsContainingTerm, termIDF, globalStats);

        Map<String, QDocument> phraseDocs = new HashMap<>();
        Set<String> docsWithMetadata = new HashSet<>();

        for (String docId : allTermDocs.keySet()) {
            if (hasExactPhrase(docId, phraseTerms)) {
                QDocument qDoc = allTermDocs.get(docId);

                int phraseCount = countExactPhraseOccurrences(docId, phraseTerms);
                double phraseTf = calculateTf(phraseCount, qDoc.getMetadata().getLength());

                TermStats phraseStats = new TermStats(containsPhraseInTitle(docId, phraseTerms.toArray(new String[0])));
                phraseStats.setTf(phraseTf);
                phraseStats.setImportanceScore(calculatePhraseImportance(phraseCount));

                qDoc.getTermStats().put(stemmedPhrase, phraseStats);
                phraseDocs.put(docId, qDoc);

                if (!docsWithMetadata.contains(docId)) {
                    setDocumentMetadata(qDoc, getDocumentPosting(docId, phraseTerms.get(0)));
                    docsWithMetadata.add(docId);
                }
            }
        }

        docsContainingTerm.put(stemmedPhrase, phraseDocs.size());
        termIDF.put(stemmedPhrase, calculatePhraseIDF(phraseDocs.size(), globalStats.getTotalDocs()));

        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);

        queryInput.setCandidateDocuments(phraseDocs);
        queryInput.setGlobalStats(globalStats);
        return queryInput;
    }

    private QueryInput processPhraseLogicalQuery(String queryString, int maxResults) {
        String[] parts;
        String operator;

        if (queryString.contains(" AND ")) {
            parts = queryString.split(" AND ");
            operator = "AND";
        } else if (queryString.contains(" OR ")) {
            parts = queryString.split(" OR ");
            operator = "OR";
        } else if (queryString.contains(" NOT ")) {
            parts = queryString.split(" NOT ");
            operator = "NOT";
        } else {
            return new QueryInput();
        }

        String leftPhrase = parts[0].trim().substring(1, parts[0].trim().length() - 1);
        String rightPhrase = parts[1].trim().substring(1, parts[1].trim().length() - 1);

        QueryInput leftInput = processPhraseQuery("\"" + leftPhrase + "\"", maxResults);
        QueryInput rightInput = processPhraseQuery("\"" + rightPhrase + "\"", maxResults);

        Map<String, QDocument> leftDocs = leftInput.getCandidateDocuments();
        Map<String, QDocument> rightDocs = rightInput.getCandidateDocuments();

        Map<String, QDocument> resultDocs = new HashMap<>();
        GlobalStats globalStats = leftInput.getGlobalStats();

        switch (operator) {
            case "AND":
                for (String docId : leftDocs.keySet()) {
                    if (rightDocs.containsKey(docId)) {
                        resultDocs.put(docId, mergeDocuments(leftDocs.get(docId), rightDocs.get(docId)));
                    }
                }
                break;
            case "OR":
                resultDocs.putAll(leftDocs);
                for (String docId : rightDocs.keySet()) {
                    if (resultDocs.containsKey(docId)) {
                        resultDocs.put(docId, mergeDocuments(resultDocs.get(docId), rightDocs.get(docId)));
                    } else {
                        resultDocs.put(docId, rightDocs.get(docId));
                    }
                }
                break;
            case "NOT":
                for (String docId : leftDocs.keySet()) {
                    if (!rightDocs.containsKey(docId)) {
                        resultDocs.put(docId, leftDocs.get(docId));
                    }
                }
                break;
        }

        List<String> queryTerms = new ArrayList<>();
        queryTerms.add(String.join(" ", preprocessQuery(leftPhrase)));
        queryTerms.add(String.join(" ", preprocessQuery(rightPhrase)));

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();

        docsContainingTerm.putAll(leftInput.getGlobalStats().getDocsContainingTerm());
        docsContainingTerm.putAll(rightInput.getGlobalStats().getDocsContainingTerm());

        termIDF.putAll(leftInput.getGlobalStats().getTermIDF());
        termIDF.putAll(rightInput.getGlobalStats().getTermIDF());

        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);

        QueryInput result = new QueryInput();
        result.setQueryTerms(queryTerms);
        result.setCandidateDocuments(resultDocs);
        result.setGlobalStats(globalStats);

        return result;
    }

    private int countExactPhraseOccurrences(String docId, List<String> phraseTerms) {
        Map<String, List<Integer>> termPositions = new HashMap<>();
        
        for (String term : phraseTerms) {
            Document wordDoc = wordIndexCollection.find(Filters.and(
                Filters.eq("word", term),
                Filters.eq("postings.doc_id", docId)
            )).first();
            
            if (wordDoc == null) return 0;
            
            List<Document> postings = wordDoc.getList("postings", Document.class);
            for (Document posting : postings) {
                if (docId.equals(posting.getString("doc_id"))) {
                    termPositions.put(term, posting.getList("positions", Integer.class));
                    break;
                }
            }
        }

        if (termPositions.size() != phraseTerms.size()) return 0;

        int count = 0;
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
            if (match) count++;
        }
        return count;
    }

    private boolean containsPhraseInTitle(String docId, String[] originalTerms) {
        Document doc = documentsCollection.find(Filters.eq("doc_id", docId)).first();
        if (doc == null) return false;
        
        String title = doc.getString("title");
        if (title == null) return false;
        
        String titleLower = title.toLowerCase();
        String originalPhrase = String.join(" ", originalTerms);
        return titleLower.contains(originalPhrase.toLowerCase());
    }

    private double calculateTf(int termCount, int docLength) {
        if (docLength == 0) return 0;
        return (double) termCount / docLength;
    }

    private double calculatePhraseImportance(int phraseCount) {
        return 1.0 + (0.1 * phraseCount);
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
        return docsWithPhrase == 0 ? 0 : Math.log((double) totalDocs / (1 + docsWithPhrase));
    }

    private QDocument mergeDocuments(QDocument doc1, QDocument doc2) {
        QDocument merged = new QDocument();
        
        merged.setMetadata(doc1.getMetadata());
        
        Map<String, TermStats> mergedStats = new HashMap<>();
        if (doc1.getTermStats() != null) {
            mergedStats.putAll(doc1.getTermStats());
        }
        if (doc2.getTermStats() != null) {
            for (Map.Entry<String, TermStats> entry : doc2.getTermStats().entrySet()) {
                if (mergedStats.containsKey(entry.getKey())) {
                    TermStats existing = mergedStats.get(entry.getKey());
                    if (entry.getValue().getTf() > existing.getTf()) {
                        mergedStats.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    mergedStats.put(entry.getKey(), entry.getValue());
                }
            }
        }
        merged.setTermStats(mergedStats);
        
        return merged;
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
}