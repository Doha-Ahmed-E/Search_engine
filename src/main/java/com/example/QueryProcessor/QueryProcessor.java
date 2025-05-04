package com.example.QueryProcessor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.example.DatabaseConnection;
import com.example.Indexer.Stemmer;
import com.example.Indexer.StopWords;
import com.example.Indexer.Tokenizer;
import com.example.Ranker.QueryInput;
import com.example.Ranker.RankedDocument;
import com.example.Ranker.ParallelRanker;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class QueryProcessor {
    private final MongoCollection<Document> invertedIndexCollection;
    private final MongoCollection<Document> pagesCollection;
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;
    private final ExecutorService executorService;

    public QueryProcessor() {
        MongoDatabase database = DatabaseConnection.getDatabase();
        this.invertedIndexCollection = database.getCollection("inverted_index");
        this.pagesCollection = database.getCollection("pages");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public List<RankedDocument> processQuery(String queryString) {
        try {
            queryString = queryString.trim();
            if (queryString.isEmpty()) {
                return Collections.emptyList();
            }

            QueryInput queryInput;
            long startTime = System.currentTimeMillis();
            
            if (queryString.startsWith("\"")) {
                if (isPhraseLogicalQuery(queryString)) {
                    queryInput = processPhraseLogicalQuery(queryString);
                } else {
                    queryInput = processPhraseQuery(queryString);
                }
            } else {
                queryInput = processTermQuery(queryString);
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("Query processing time: " + (endTime - startTime) + " ms");

            if (queryInput.getCandidateDocuments().isEmpty()) {
                return Collections.emptyList();
            }

            ParallelRanker ranker = new ParallelRanker();
            startTime = System.currentTimeMillis();
            List<RankedDocument> rankedDocuments = ranker.rank(queryInput);
            endTime = System.currentTimeMillis();
            System.out.println("Ranking time: " + (endTime - startTime) + " ms");
            
            return rankedDocuments.stream()
                    .sorted(Comparator.comparingDouble(RankedDocument::getScore).reversed())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            System.err.println("Error processing query: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private QueryInput processTermQuery(String queryString) {
        List<String> queryTerms = preprocessQuery(queryString);
        QueryInput queryInput = new QueryInput();
        if (queryTerms.isEmpty()) {
            return queryInput;
        }

        queryInput.setQueryTerms(queryTerms);
        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();
        GlobalStats globalStats = new GlobalStats();
        
        Map<String, QDocument> candidateDocs = collectDocumentInfo(queryTerms, docsContainingTerm, termIDF, globalStats);
        
        queryInput.setCandidateDocuments(candidateDocs);
        queryInput.setGlobalStats(globalStats);
        return queryInput;
    }

    private Map<String, QDocument> collectDocumentInfo(List<String> queryTerms,
            Map<String, Integer> docsContainingTerm, Map<String, Double> termIDF,
            GlobalStats globalStats) {
        Map<String, QDocument> candidateDocs = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String term : queryTerms) {
            futures.add(CompletableFuture.runAsync(() -> {
                Document wordDoc = invertedIndexCollection.find(Filters.eq("word", term)).first();
                if (wordDoc == null) return;

                int docCount = wordDoc.getInteger("doc_count", 0);
                double idf = wordDoc.getDouble("idf");
                
                synchronized (docsContainingTerm) {
                    docsContainingTerm.put(term, docCount);
                    termIDF.put(term, idf);
                }

                List<Document> postings = wordDoc.getList("postings", Document.class);
                if (postings == null) return;
                
                for (Document posting : postings) {
                    String docId = posting.getString("doc_id");
                    QDocument qDoc = candidateDocs.computeIfAbsent(docId, k -> new QDocument());

                    TermStats termStats = new TermStats(posting.getBoolean("in_title", false));
                    termStats.setTf(posting.getDouble("tf"));
                    // termStats.setPositions(posting.getList("positions", Integer.class, Collections.emptyList()));
                    
                    synchronized (qDoc) {
                        Map<String, TermStats> termStatsMap = qDoc.getTermStats();
                        if (termStatsMap == null) {
                            termStatsMap = new HashMap<>();
                            qDoc.setTermStats(termStatsMap);
                        }
                        termStatsMap.put(term, termStats);

                        if (qDoc.getMetadata() == null) {
                            setDocumentMetadata(qDoc, posting, docId);
                        }
                    }
                }
            }, executorService));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        globalStats.setTotalDocs((int) pagesCollection.countDocuments());
        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);
        
        return candidateDocs;
    }

    private void setDocumentMetadata(QDocument qDoc, Document posting, String docId) {
        Document pageDoc = pagesCollection.find(Filters.eq("doc_id", docId)).first();
        if (pageDoc == null) return;

        Metadata metadata = new Metadata();
        metadata.setUrl(posting.getString("url"));
        metadata.setTitle(posting.getString("title"));
        metadata.setPopularity(pageDoc.getDouble("popularity"));
        metadata.setSnippet(getBestSnippet(posting.getList("snippets", String.class)));
        // metadata.setTimestamp(pageDoc.getLong("timestamp"));
        
        qDoc.setMetadata(metadata);
    }

    private String getBestSnippet(List<String> snippets) {
        if (snippets == null || snippets.isEmpty()) {
            return "No preview available";
        }
        // Return the longest snippet
        return snippets.stream()
                .max(Comparator.comparingInt(String::length))
                .orElse(snippets.get(0));
    }

    private boolean isPhraseLogicalQuery(String queryString) {
        String trimmed = queryString.trim();
        int andIndex = trimmed.indexOf(" AND ");
        int orIndex = trimmed.indexOf(" OR ");
        int notIndex = trimmed.indexOf(" NOT ");

        if (andIndex > 0) return isProperlyQuoted(trimmed, andIndex, 5);
        if (orIndex > 0) return isProperlyQuoted(trimmed, orIndex, 4);
        if (notIndex > 0) return isProperlyQuoted(trimmed, notIndex, 5);
        return false;
    }

    private boolean isProperlyQuoted(String query, int opIndex, int opLength) {
        String left = query.substring(0, opIndex).trim();
        String right = query.substring(opIndex + opLength).trim();
        return left.startsWith("\"") && left.endsWith("\"") && right.startsWith("\"") 
                && right.endsWith("\"");
    }

    private QueryInput processPhraseQuery(String queryString) {
        String phrase = queryString.trim().substring(1, queryString.length() - 1).trim();
        if (phrase.isEmpty()) return new QueryInput();

        List<String> phraseTerms = preprocessQuery(phrase);
        if (phraseTerms.size() < 2) {
            return processTermQuery(phrase);
        }

        Set<String> commonDocIds = findDocumentsWithAllTerms(phraseTerms);
        if (commonDocIds.isEmpty()) {
            return new QueryInput();
        }

        Map<String, QDocument> phraseDocs = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String docId : commonDocIds) {
            futures.add(CompletableFuture.runAsync(() -> {
                List<Document> termPostings = new ArrayList<>();
                for (String term : phraseTerms) {
                    Document wordDoc = invertedIndexCollection.find(Filters.and(
                            Filters.eq("word", term),
                            Filters.eq("postings.doc_id", docId)
                    )).first();
                    if (wordDoc == null) return;
                    termPostings.add(wordDoc);
                }

                if (termPostings.size() == phraseTerms.size()) {
                    QDocument qDoc = new QDocument();
                    Document firstPosting = termPostings.get(0).getList("postings", Document.class).get(0);
                    setDocumentMetadata(qDoc, firstPosting, docId);

                    // Check for exact phrase matches
                    int phraseCount = countPhraseOccurrences(phraseTerms, docId);
                    if (phraseCount > 0) {
                        String phraseKey = String.join(" ", phraseTerms);
                        TermStats phraseStats = new TermStats(false);
                        phraseStats.setTf((double) phraseCount / qDoc.getMetadata().getLength());
                        qDoc.getTermStats().put(phraseKey, phraseStats);
                        phraseDocs.put(docId, qDoc);
                    }
                }
            }, executorService));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return buildQueryInput(phraseTerms, phraseDocs);
    }

    private int countPhraseOccurrences(List<String> phraseTerms, String docId) {
        List<List<Integer>> allPositions = new ArrayList<>();
        
        for (String term : phraseTerms) {
            Document wordDoc = invertedIndexCollection.find(Filters.and(
                    Filters.eq("word", term),
                    Filters.eq("postings.doc_id", docId)
            )).first();
            
            if (wordDoc == null) return 0;
            
            List<Integer> positions = wordDoc.getList("postings", Document.class).get(0)
                    .getList("positions", Integer.class);
            allPositions.add(positions);
        }

        if (allPositions.isEmpty()) return 0;

        int count = 0;
        for (int pos : allPositions.get(0)) {
            boolean match = true;
            for (int i = 1; i < allPositions.size(); i++) {
                if (!allPositions.get(i).contains(pos + i)) {
                    match = false;
                    break;
                }
            }
            if (match) count++;
        }
        return count;
    }

    private Set<String> findDocumentsWithAllTerms(List<String> terms) {
        Set<String> commonDocIds = null;

        for (String term : terms) {
            Document wordDoc = invertedIndexCollection.find(Filters.eq("word", term))
                    .projection(new Document("postings.doc_id", 1)).first();

            if (wordDoc == null) return Collections.emptySet();

            Set<String> termDocIds = wordDoc.getList("postings", Document.class).stream()
                    .map(posting -> posting.getString("doc_id"))
                    .collect(Collectors.toSet());

            if (commonDocIds == null) {
                commonDocIds = new HashSet<>(termDocIds);
            } else {
                commonDocIds.retainAll(termDocIds);
                if (commonDocIds.isEmpty()) break;
            }
        }

        return commonDocIds != null ? commonDocIds : Collections.emptySet();
    }

    private QueryInput buildQueryInput(List<String> phraseTerms, Map<String, QDocument> phraseDocs) {
        QueryInput queryInput = new QueryInput();
        String stemmedPhrase = String.join(" ", phraseTerms);
        queryInput.setQueryTerms(Collections.singletonList(stemmedPhrase));

        GlobalStats globalStats = new GlobalStats();
        globalStats.setTotalDocs((int) pagesCollection.countDocuments());

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();

        for (String term : phraseTerms) {
            Document wordDoc = invertedIndexCollection.find(Filters.eq("word", term))
                    .projection(new Document("doc_count", 1).append("idf", 1)).first();

            if (wordDoc != null) {
                docsContainingTerm.put(term, wordDoc.getInteger("doc_count"));
                termIDF.put(term, wordDoc.getDouble("idf"));
            }
        }

        docsContainingTerm.put(stemmedPhrase, phraseDocs.size());
        termIDF.put(stemmedPhrase, Math.log((double) globalStats.getTotalDocs() / (1 + phraseDocs.size())));

        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);
        queryInput.setGlobalStats(globalStats);
        queryInput.setCandidateDocuments(phraseDocs);

        return queryInput;
    }

    private QueryInput processPhraseLogicalQuery(String queryString) {
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

        CompletableFuture<QueryInput> leftFuture = CompletableFuture
                .supplyAsync(() -> processPhraseQuery(parts[0].trim()), executorService);
        CompletableFuture<QueryInput> rightFuture = CompletableFuture
                .supplyAsync(() -> processPhraseQuery(parts[1].trim()), executorService);

        try {
            QueryInput leftInput = leftFuture.get();
            QueryInput rightInput = rightFuture.get();

            return combineLogicalResults(leftInput, rightInput, operator);
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            return new QueryInput();
        }
    }

    private QueryInput combineLogicalResults(QueryInput leftInput, QueryInput rightInput,
            String operator) {
        Map<String, QDocument> leftDocs = leftInput.getCandidateDocuments();
        Map<String, QDocument> rightDocs = rightInput.getCandidateDocuments();
        Map<String, QDocument> resultDocs = new HashMap<>();

        switch (operator) {
            case "AND":
                leftDocs.keySet().stream().filter(rightDocs::containsKey)
                        .forEach(docId -> resultDocs.put(docId,
                                mergeDocuments(leftDocs.get(docId), rightDocs.get(docId))));
                break;
            case "OR":
                resultDocs.putAll(leftDocs);
                rightDocs.forEach(
                        (docId, doc) -> resultDocs.merge(docId, doc, this::mergeDocuments));
                break;
            case "NOT":
                leftDocs.keySet().stream().filter(docId -> !rightDocs.containsKey(docId))
                        .forEach(docId -> resultDocs.put(docId, leftDocs.get(docId)));
                break;
        }

        QueryInput result = new QueryInput();
        result.setQueryTerms(combineQueryTerms(leftInput, rightInput));
        result.setCandidateDocuments(resultDocs);
        result.setGlobalStats(combineGlobalStats(leftInput, rightInput));

        return result;
    }

    private List<String> combineQueryTerms(QueryInput left, QueryInput right) {
        List<String> combined = new ArrayList<>(left.getQueryTerms());
        combined.addAll(right.getQueryTerms());
        return combined;
    }

    private GlobalStats combineGlobalStats(QueryInput left, QueryInput right) {
        GlobalStats combined = new GlobalStats();
        combined.setTotalDocs(left.getGlobalStats().getTotalDocs());

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        docsContainingTerm.putAll(left.getGlobalStats().getDocsContainingTerm());
        docsContainingTerm.putAll(right.getGlobalStats().getDocsContainingTerm());
        combined.setDocsContainingTerm(docsContainingTerm);

        Map<String, Double> termIDF = new HashMap<>();
        termIDF.putAll(left.getGlobalStats().getTermIDF());
        termIDF.putAll(right.getGlobalStats().getTermIDF());
        combined.setTermIDF(termIDF);

        return combined;
    }

    private QDocument mergeDocuments(QDocument doc1, QDocument doc2) {
        QDocument merged = new QDocument();
        merged.setMetadata(doc1.getMetadata());

        Map<String, TermStats> mergedStats = new HashMap<>();
        if (doc1.getTermStats() != null) {
            mergedStats.putAll(doc1.getTermStats());
        }
        if (doc2.getTermStats() != null) {
            doc2.getTermStats().forEach((term, stats) -> {
                mergedStats.merge(term, stats, (oldStats, newStats) -> {
                    if (newStats.getTf() > oldStats.getTf()) {
                        return newStats;
                    }
                    return oldStats;
                });
            });
        }
        merged.setTermStats(mergedStats);

        return merged;
    }

    private List<String> preprocessQuery(String queryString) {
        List<String> tokens = tokenizer.tokenize(queryString);
        tokens = stopWords.removeStopWords(tokens);
        return tokens.stream()
                .map(stemmer::stem)
                .filter(Objects::nonNull)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}