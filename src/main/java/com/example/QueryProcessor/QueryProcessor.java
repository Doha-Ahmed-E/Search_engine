package com.example.QueryProcessor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.example.DatabaseConnection;
import com.example.Indexer.Stemmer;
import com.example.Indexer.StopWords;
import com.example.Indexer.Tokenizer;
import com.example.Ranker.QueryInput;
import com.example.Ranker.RankedDocument;
import com.example.util.TypeSafeUtil;
import com.example.Ranker.ParallelRanker;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class QueryProcessor
{
    private final MongoCollection<Document> wordIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;
    private final ExecutorService executorService;
    private final Map<String, Map<String, List<Integer>>> positionCache;

    public QueryProcessor()
    {
        this.wordIndexCollection = DatabaseConnection.getDatabase().getCollection("inverted_index");
        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();
        this.executorService = Executors.newFixedThreadPool(4);
        this.positionCache = new ConcurrentHashMap<>();
    }

    public List<RankedDocument> processQuery(String queryString)
    {
        try
        {
            queryString = queryString.trim();
            QueryInput queryInput;
            if (queryString.startsWith("\""))
            {
                if (isPhraseLogicalQuery(queryString))
                {
                    queryInput = processPhraseLogicalQuery(queryString);
                }
                else
                {
                    queryInput = processPhraseQuery(queryString);
                }
            }
            else
            {
                queryInput = processTermQuery(queryString);
            }

            ParallelRanker ranker = new ParallelRanker();
            return ranker.rank(queryInput);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    private QueryInput processTermQuery(String queryString)
    {
        List<String> queryTerms = preprocessQuery(queryString);
        QueryInput queryInput = new QueryInput();
        if (queryTerms.isEmpty())
            return queryInput;

        queryInput.setQueryTerms(queryTerms);
        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();
        GlobalStats globalStats = new GlobalStats();
        Map<String, QDocument> candidateDocs =
                collectDocumentInfo(queryTerms, docsContainingTerm, termIDF, globalStats);

        queryInput.setCandidateDocuments(candidateDocs);
        queryInput.setGlobalStats(globalStats);
        return queryInput;
    }

    private boolean isPhraseLogicalQuery(String queryString)
    {
        String trimmed = queryString.trim();
        int andIndex = trimmed.indexOf(" AND ");
        int orIndex = trimmed.indexOf(" OR ");
        int notIndex = trimmed.indexOf(" NOT ");

        if (andIndex > 0)
            return isProperlyQuoted(trimmed, andIndex, 5);
        if (orIndex > 0)
            return isProperlyQuoted(trimmed, orIndex, 4);
        if (notIndex > 0)
            return isProperlyQuoted(trimmed, notIndex, 5);
        return false;
    }

    private boolean isProperlyQuoted(String query, int opIndex, int opLength)
    {
        String left = query.substring(0, opIndex).trim();
        String right = query.substring(opIndex + opLength).trim();
        return left.startsWith("\"") && left.endsWith("\"") && right.startsWith("\"")
                && right.endsWith("\"");
    }

    private QueryInput processPhraseQuery(String queryString)
    {
        String phrase = queryString.trim().substring(1, queryString.length() - 1).trim();
        if (phrase.isEmpty())
            return new QueryInput();

        List<String> phraseTerms = preprocessQuery(phrase);
        if (phraseTerms.size() < 2)
        {
            return processTermQuery(phrase);
        }

        Set<String> commonDocIds = findDocumentsWithAllTerms(phraseTerms);
        if (commonDocIds.isEmpty())
        {
            return new QueryInput();
        }

        Map<String, Map<String, List<Integer>>> termPositionsMap =
                fetchTermPositions(phraseTerms, commonDocIds);
        Map<String, QDocument> phraseDocs =
                findExactPhraseMatches(phraseTerms, commonDocIds, termPositionsMap);

        return buildQueryInput(phraseTerms, phraseDocs);
    }

    private Set<String> findDocumentsWithAllTerms(List<String> terms)
    {
        Set<String> commonDocIds = null;

        for (String term : terms)
        {
            Document wordDoc = wordIndexCollection.find(Filters.eq("word", term))
                    .projection(new Document("postings.doc_id", 1)).first();

            if (wordDoc == null)
                return Collections.emptySet();

            Set<String> termDocIds = wordDoc.getList("postings", Document.class).stream()
                    .map(posting -> posting.getString("doc_id")).collect(Collectors.toSet());

            if (commonDocIds == null)
            {
                commonDocIds = new HashSet<>(termDocIds);
            }
            else
            {
                commonDocIds.retainAll(termDocIds);
                if (commonDocIds.isEmpty())
                    break;
            }
        }

        return commonDocIds != null ? commonDocIds : Collections.emptySet();
    }

    private Map<String, Map<String, List<Integer>>> fetchTermPositions(List<String> terms,
            Set<String> docIds)
    {
        Map<String, Map<String, List<Integer>>> termPositionsMap = new HashMap<>();
        List<Future<?>> futures = new ArrayList<>();

        for (String term : terms)
        {
            futures.add(executorService.submit(() -> {
                Map<String, List<Integer>> positions = positionCache.computeIfAbsent(term, k -> {
                    Document wordDoc = wordIndexCollection.find(Filters.eq("word", term))
                            .projection(new Document("postings.doc_id", 1)
                                    .append("postings.positions", 1))
                            .first();

                    if (wordDoc == null)
                        return Collections.emptyMap();

                    return wordDoc.getList("postings", Document.class).stream()
                            .filter(posting -> docIds.contains(posting.getString("doc_id")))
                            .collect(Collectors.toMap(posting -> posting.getString("doc_id"),
                                    posting -> posting.getList("positions", Integer.class)));
                });

                synchronized (termPositionsMap)
                {
                    termPositionsMap.put(term, positions);
                }
            }));
        }

        for (Future<?> future : futures)
        {
            try
            {
                future.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                Thread.currentThread().interrupt();
                return Collections.emptyMap();
            }
        }

        return termPositionsMap;
    }

    private Map<String, QDocument> findExactPhraseMatches(List<String> phraseTerms,
            Set<String> docIds, Map<String, Map<String, List<Integer>>> termPositionsMap)
    {
        Map<String, QDocument> phraseDocs = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String docId : docIds)
        {
            futures.add(CompletableFuture.runAsync(() -> {
                int phraseCount = countExactPhraseOccurrences(phraseTerms, termPositionsMap, docId);
                if (phraseCount > 0)
                {
                    QDocument qDoc = createPhraseDocument(docId, phraseTerms, phraseCount);
                    phraseDocs.put(docId, qDoc);
                }
            }, executorService));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return phraseDocs;
    }

    private int countExactPhraseOccurrences(List<String> phraseTerms,
            Map<String, Map<String, List<Integer>>> termPositionsMap, String docId)
    {
        List<Integer> firstTermPositions = termPositionsMap.get(phraseTerms.get(0)).get(docId);
        if (firstTermPositions == null)
            return 0;

        int count = 0;
        for (int pos : firstTermPositions)
        {
            boolean match = true;
            for (int i = 1; i < phraseTerms.size(); i++)
            {
                List<Integer> positions = termPositionsMap.get(phraseTerms.get(i)).get(docId);
                if (positions == null || !positions.contains(pos + i))
                {
                    match = false;
                    break;
                }
            }
            if (match)
                count++;
        }
        return count;
    }

    private QDocument createPhraseDocument(String docId, List<String> phraseTerms, int phraseCount)
    {
        QDocument qDoc = new QDocument();
        String stemmedPhrase = String.join(" ", phraseTerms);

        Document doc =
                documentsCollection
                        .find(Filters.eq("doc_id", docId)).projection(new Document("url", 1)
                                .append("length", 1).append("timestamp", 1).append("title", 1))
                        .first();

        if (doc != null)
        {
            Metadata metadata = new Metadata();
            metadata.setUrl(doc.getString("url"));
            metadata.setPopularity(0.5);
            metadata.setLength(doc.getInteger("length", 0));
            Long timestamp = doc.getLong("timestamp");
            metadata.setPublishDate(timestamp != null
                    ? new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp))
                    : "");
            qDoc.setMetadata(metadata);

            String title = doc.getString("title");
            boolean inTitle = title != null
                    && title.toLowerCase().contains(String.join(" ", phraseTerms).toLowerCase());

            double phraseTf = calculateTf(phraseCount, metadata.getLength());
            TermStats phraseStats = new TermStats(inTitle);
            phraseStats.setTf(phraseTf);
            phraseStats.setImportanceScore(calculatePhraseImportance(phraseCount));

            Map<String, TermStats> termStats = new HashMap<>();
            termStats.put(stemmedPhrase, phraseStats);
            qDoc.setTermStats(termStats);
        }

        return qDoc;
    }

    private QueryInput buildQueryInput(List<String> phraseTerms, Map<String, QDocument> phraseDocs)
    {
        QueryInput queryInput = new QueryInput();
        String stemmedPhrase = String.join(" ", phraseTerms);
        queryInput.setQueryTerms(Collections.singletonList(stemmedPhrase));

        GlobalStats globalStats = new GlobalStats();
        globalStats.setTotalDocs((int) documentsCollection.countDocuments());

        Map<String, Integer> docsContainingTerm = new HashMap<>();
        Map<String, Double> termIDF = new HashMap<>();

        for (String term : phraseTerms)
        {
            Document wordDoc = wordIndexCollection.find(Filters.eq("word", term))
                    .projection(new Document("doc_count", 1).append("idf", 1)).first();

            if (wordDoc != null)
            {
                docsContainingTerm.put(term, wordDoc.getInteger("doc_count"));
                termIDF.put(term, wordDoc.getDouble("idf"));
            }
        }

        docsContainingTerm.put(stemmedPhrase, phraseDocs.size());
        termIDF.put(stemmedPhrase,
                calculatePhraseIDF(phraseDocs.size(), globalStats.getTotalDocs()));

        globalStats.setDocsContainingTerm(docsContainingTerm);
        globalStats.setTermIDF(termIDF);
        queryInput.setGlobalStats(globalStats);
        queryInput.setCandidateDocuments(phraseDocs);

        return queryInput;
    }

    private QueryInput processPhraseLogicalQuery(String queryString)
    {
        String[] parts;
        String operator;

        if (queryString.contains(" AND "))
        {
            parts = queryString.split(" AND ");
            operator = "AND";
        }
        else if (queryString.contains(" OR "))
        {
            parts = queryString.split(" OR ");
            operator = "OR";
        }
        else if (queryString.contains(" NOT "))
        {
            parts = queryString.split(" NOT ");
            operator = "NOT";
        }
        else
        {
            return new QueryInput();
        }

        CompletableFuture<QueryInput> leftFuture = CompletableFuture
                .supplyAsync(() -> processPhraseQuery(parts[0].trim()), executorService);
        CompletableFuture<QueryInput> rightFuture = CompletableFuture
                .supplyAsync(() -> processPhraseQuery(parts[1].trim()), executorService);

        try
        {
            QueryInput leftInput = leftFuture.get();
            QueryInput rightInput = rightFuture.get();

            return combineLogicalResults(leftInput, rightInput, operator);
        }
        catch (InterruptedException | ExecutionException e)
        {
            Thread.currentThread().interrupt();
            return new QueryInput();
        }
    }

    private QueryInput combineLogicalResults(QueryInput leftInput, QueryInput rightInput,
            String operator)
    {
        Map<String, QDocument> leftDocs = leftInput.getCandidateDocuments();
        Map<String, QDocument> rightDocs = rightInput.getCandidateDocuments();
        Map<String, QDocument> resultDocs = new HashMap<>();

        switch (operator)
        {
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

    private List<String> combineQueryTerms(QueryInput left, QueryInput right)
    {
        List<String> combined = new ArrayList<>(left.getQueryTerms());
        combined.addAll(right.getQueryTerms());
        return combined;
    }

    private GlobalStats combineGlobalStats(QueryInput left, QueryInput right)
    {
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

    private double calculateTf(int termCount, int docLength)
    {
        return docLength == 0 ? 0 : (double) termCount / docLength;
    }

    private double calculatePhraseImportance(int phraseCount)
    {
        return 1.0 + (0.1 * phraseCount);
    }

    private double calculatePhraseIDF(int docsWithPhrase, int totalDocs)
    {
        return docsWithPhrase == 0 ? 0 : Math.log((double) totalDocs / (1 + docsWithPhrase));
    }

    private QDocument mergeDocuments(QDocument doc1, QDocument doc2)
    {
        QDocument merged = new QDocument();
        merged.setMetadata(doc1.getMetadata());

        Map<String, TermStats> mergedStats = new HashMap<>();
        if (doc1.getTermStats() != null)
        {
            mergedStats.putAll(doc1.getTermStats());
        }
        if (doc2.getTermStats() != null)
        {
            for (Map.Entry<String, TermStats> entry : doc2.getTermStats().entrySet())
            {
                if (mergedStats.containsKey(entry.getKey()))
                {
                    TermStats existing = mergedStats.get(entry.getKey());
                    if (entry.getValue().getTf() > existing.getTf())
                    {
                        mergedStats.put(entry.getKey(), entry.getValue());
                    }
                }
                else
                {
                    mergedStats.put(entry.getKey(), entry.getValue());
                }
            }
        }
        merged.setTermStats(mergedStats);

        return merged;
    }

    private List<String> preprocessQuery(String queryString)
    {
        List<String> tokens = tokenizer.tokenize(queryString);
        tokens = stopWords.removeStopWords(tokens);
        List<String> stemmedTokens = new ArrayList<>();
        for (String token : tokens)
        {
            String stemmed = stemmer.stem(token);
            if (stemmed != null && !stemmed.isEmpty())
            {
                stemmedTokens.add(stemmed);
            }
        }
        return stemmedTokens;
    }

    private Map<String, QDocument> collectDocumentInfo(List<String> queryTerms,
            Map<String, Integer> docsContainingTerm, Map<String, Double> termIDF,
            GlobalStats globalStats)
    {
        Map<String, QDocument> candidateDocs = new HashMap<>();
        Set<String> docsWithMetadata = new HashSet<>();

        for (String term : queryTerms)
        {
            Document wordDoc = wordIndexCollection.find(Filters.eq("word", term)).first();
            if (wordDoc == null)
                continue;

            docsContainingTerm.put(term, wordDoc.getInteger("doc_count"));
            termIDF.put(term, wordDoc.getDouble("idf"));

            List<Document> postings = wordDoc.getList("postings", Document.class);
            if (postings == null)
                continue;
            for (Document posting : postings)
            {
                String docId = posting.getString("doc_id");
                QDocument qDoc = candidateDocs.computeIfAbsent(docId, k -> new QDocument());

                Map<String, TermStats> termInfoMap = qDoc.getTermStats();
                if (termInfoMap == null)
                {
                    termInfoMap = new HashMap<>();
                    qDoc.setTermStats(termInfoMap);
                }

                TermStats termStats = new TermStats(posting.getBoolean("in_title"));
                termStats.setTf(posting.getDouble("tf"));
                termStats.setImportanceScore(posting.getDouble("importance_score"));
                termInfoMap.put(term, termStats);

                if (!docsWithMetadata.contains(docId))
                {
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

    private void setDocumentMetadata(QDocument qDoc, Document posting)
    {
        if (posting == null)
            return;

        // Get pre-computed snippets from the posting
        List<String> snippets = TypeSafeUtil.safeStringList(posting.get("snippet"));
        String bestSnippet =
                selectBestSnippetForTerms(new ArrayList<>(qDoc.getTermStats().keySet()), snippets);

        Metadata metadata = new Metadata();
        metadata.setUrl(posting.getString("url"));
        metadata.setPopularity(
                posting.containsKey("popularity") ? posting.getDouble("popularity") : 0.0);
        metadata.setTitle(posting.getString("title"));
        metadata.setSnippet(bestSnippet);
        metadata.setLength(posting.getInteger("length", 0));

        qDoc.setMetadata(metadata);
    }

    private String selectBestSnippetForTerms(List<String> queryTerms, List<String> snippets)
    {
        if (snippets == null || snippets.isEmpty())
            return "No preview available";

        // If only one snippet, return it
        if (snippets.size() == 1)
            return snippets.get(0);

        // If we have very few terms and snippets, consider joining them
        if (queryTerms.size() > 1 && snippets.size() <= 5)
        {
            return joinSnippetsForDifferentTerms(queryTerms, snippets);
        }

        // Otherwise use existing scoring logic for single best snippet
        Map<String, Integer> snippetScores = new HashMap<>();
        for (String snippet : snippets)
        {
            int score = 0;
            String lowerSnippet = snippet.toLowerCase();

            for (String term : queryTerms)
            {
                if (lowerSnippet.contains(term.toLowerCase()))
                {
                    score++;
                    // Bonus points if the term is highlighted
                    String highlightPattern = "<strong>" + term + "</strong>";
                    if (lowerSnippet.contains(highlightPattern.toLowerCase()))
                        score += 2;
                }
            }
            snippetScores.put(snippet, score);
        }
        // Return the snippet with the highest score
        return snippets.stream().max(Comparator.comparingInt(snippetScores::get))
                .orElse(snippets.get(0)); // Fallback to first snippet if scoring fails
    }

    private String joinSnippetsForDifferentTerms(List<String> queryTerms, List<String> snippets)
    {
        // Track best snippet for each query term
        Map<String, String> bestSnippetPerTerm = new HashMap<>();

        // Find best snippet for each term
        for (String term : queryTerms)
        {
            String bestForTerm = null;
            int bestScore = -1;

            for (String snippet : snippets)
            {
                String lowerSnippet = snippet.toLowerCase();
                String lowerTerm = term.toLowerCase();

                if (lowerSnippet.contains(lowerTerm))
                {
                    int score = 0;

                    // Score based on how central the term is in the snippet
                    int termPos = lowerSnippet.indexOf(lowerTerm);
                    int snippetCenter = lowerSnippet.length() / 2;
                    int distanceFromCenter = Math.abs(termPos - snippetCenter);
                    score += 100 - Math.min(100, distanceFromCenter);

                    // Bonus if term is highlighted
                    String highlightPattern = "<strong>" + term + "</strong>";
                    if (lowerSnippet.contains(highlightPattern.toLowerCase()))
                        score += 50;

                    // Update best snippet for this term if needed
                    if (score > bestScore)
                    {
                        bestScore = score;
                        bestForTerm = snippet;
                    }
                }
            }

            // Store best snippet for this term
            if (bestForTerm != null)
                bestSnippetPerTerm.put(term, bestForTerm);
        }

        // If we couldn't find specific snippets for terms, fall back to default method
        if (bestSnippetPerTerm.isEmpty())
            return snippets.get(0);


        // Join distinct snippets with a separator
        List<String> distinctSnippets =
                new ArrayList<>(new LinkedHashSet<>(bestSnippetPerTerm.values()));

        // Limit to at most 3 snippets to avoid too long results
        if (distinctSnippets.size() > 3)
            distinctSnippets = distinctSnippets.subList(0, 3);

        // Join with separator
        return String.join(" ... | ... ", distinctSnippets);
    }

    // Helper method to print results in a readable format
    private static void printQueryResults(QueryInput result)
    {
        // Print query terms
        System.out.println("Query terms: " + result.getQueryTerms());

        // Print global stats
        GlobalStats stats = result.getGlobalStats();
        System.out.println("\nGlobal statistics:");
        System.out.println("  Total documents: " + stats.getTotalDocs());
        System.out.println("  Documents containing terms:");

        Map<String, Integer> docsContainingTerm = stats.getDocsContainingTerm();
        if (docsContainingTerm != null)
        {
            for (Map.Entry<String, Integer> entry : docsContainingTerm.entrySet())
            {
                System.out.println("    " + entry.getKey() + ": " + entry.getValue() + " docs");
            }
        }

        // Print IDF values
        System.out.println("\n  Term IDF values:");
        Map<String, Double> termIDF = stats.getTermIDF();
        if (termIDF != null)
        {
            for (Map.Entry<String, Double> entry : termIDF.entrySet())
            {
                System.out.println(
                        "    " + entry.getKey() + ": " + String.format("%.4f", entry.getValue()));
            }
        }

        // Print document results
        Map<String, QDocument> docs = result.getCandidateDocuments();
        System.out.println("\nFound " + (docs != null ? docs.size() : 0) + " candidate documents:");

        if (docs != null && !docs.isEmpty())
        {
            int count = 0;
            for (Map.Entry<String, QDocument> entry : docs.entrySet())
            {
                count++;
                QDocument doc = entry.getValue();
                Metadata metadata = doc.getMetadata();

                System.out.println("\n" + count + ". Document: " + entry.getKey());

                // Print all available metadata
                if (metadata != null)
                {
                    // Print title with proper formatting
                    String title = metadata.getTitle();
                    if (title != null && !title.isEmpty())
                    {
                        System.out.println("   Title: "
                                + (title.length() > 60 ? title.substring(0, 57) + "..." : title));
                    }
                    else
                    {
                        System.out.println("   Title: [No title available]");
                    }

                    System.out.println("   URL: " + metadata.getUrl());
                    System.out.println(
                            "   Popularity: " + String.format("%.2f", metadata.getPopularity()));
                    System.out.println("   Length: " + metadata.getLength() + " words");

                    if (metadata.getPublishDate() != null && !metadata.getPublishDate().isEmpty())
                    {
                        System.out.println("   Publish date: " + metadata.getPublishDate());
                    }

                    // Print snippet if available
                    String snippet = metadata.getSnippet();
                    if (snippet != null && !snippet.isEmpty())
                    {
                        // Convert HTML formatting to asterisks for console display
                        snippet = snippet.replaceAll("<b>", "*").replaceAll("</b>", "*");
                        System.out.println("   Snippet: " + snippet);
                    }
                    else
                    {
                        System.out.println("   Snippet: [No snippet available]");
                    }
                }

                // Print term stats for this document
                Map<String, TermStats> termStats = doc.getTermStats();
                if (termStats != null && !termStats.isEmpty())
                {
                    System.out.println("   Term statistics:");
                    for (Map.Entry<String, TermStats> termEntry : termStats.entrySet())
                    {
                        TermStats ts = termEntry.getValue();
                        System.out.println("     " + termEntry.getKey() + ": tf="
                                + String.format("%.4f", ts.getTf()) + ", inTitle=" + ts.isInTitle()
                                + ", importance=" + String.format("%.2f", ts.getImportanceScore()));
                    }
                }

                // Only show first 5 documents in detail to avoid too much output
                if (count >= 5 && docs.size() > 5)
                {
                    System.out.println("\n...and " + (docs.size() - 5) + " more documents");
                    break;
                }
            }
        }
        else
        {
            System.out.println("No matching documents found.");
        }
    }

    // Main method for direct testing
    public static void main(String[] args)
    {
        try
        {
            // Initialize the QueryProcessor
            QueryProcessor processor = new QueryProcessor();

            // Default query if none provided
            String query = "stack and Java # programming maybe sql";
            if (args.length > 0)
            {
                query = args[0];
            }

            System.out.println("\nProcessing query: \"" + query + "\"");
            long startTime = System.currentTimeMillis();

            // Process the query
            QueryInput result = processor.processTermQuery(query);

            long endTime = System.currentTimeMillis();
            System.out.println("Query processing time: " + (endTime - startTime) + "ms\n");

            // Print the result details
            printQueryResults(result);

            // Save to file
            String filename = "query_result_" + System.currentTimeMillis() + ".json";
            System.out.println("\nDetailed results saved to: " + filename);

        }
        catch (Exception e)
        {
            System.err.println("Error processing query: " + e.getMessage());
            e.printStackTrace();
        }
    }
}


