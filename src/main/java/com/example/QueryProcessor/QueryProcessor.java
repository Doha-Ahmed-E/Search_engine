package com.example.QueryProcessor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import ch.qos.logback.core.boolex.Matcher;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class QueryProcessor
{
    private final MongoCollection<Document> wordIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final MongoCollection<Document> pagesCollection;
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;
    private final ExecutorService executorService;
    private final Map<String, Map<String, List<Integer>>> positionCache;
    List<String> nonStemmedQueryTerms = null;
    public QueryProcessor()
    {
        this.wordIndexCollection = DatabaseConnection.getDatabase().getCollection("inverted_index");
        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.pagesCollection = DatabaseConnection.getDatabase().getCollection("pages");
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

    private Map<String, Map<String, List<Integer>>> fetchTermPositions(List<String> terms, Set<String> docIds)
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
                    QDocument qDoc = createPhraseDocument(docId, phraseTerms, phraseCount,termPositionsMap);
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

    private QDocument createPhraseDocument(String docId, List<String> phraseTerms, int phraseCount, 
                                      Map<String, Map<String, List<Integer>>> termPositionsMap) {
    QDocument qDoc = new QDocument();
    String stemmedPhrase = String.join(" ", phraseTerms);

    Document doc = pagesCollection
            .find(Filters.eq("doc_id", docId))
            .projection(new Document("url", 1)
                .append("timestamp", 1)
                .append("title", 1)
                .append("popularity", 1))
            .first();
    
    if (doc != null) {
        Metadata metadata = new Metadata();
        metadata.setUrl(doc.getString("url"));
        metadata.setPopularity(doc.getDouble("popularity"));
        Long timestamp = doc.getLong("timestamp");
        metadata.setPublishDate(timestamp != null
            ? new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp))
            : "");
        qDoc.setMetadata(metadata);
        
        String title = doc.getString("title");
        metadata.setTitle(title);
        
        boolean inTitle = title != null
                && title.toLowerCase().contains(String.join(" ", phraseTerms).toLowerCase());

        double phraseTf = calculateTf(phraseCount, metadata.getLength());
        TermStats phraseStats = new TermStats(inTitle);
        phraseStats.setTf(phraseTf);
        phraseStats.setImportanceScore(calculatePhraseImportance(phraseCount));

        Map<String, TermStats> termStats = new HashMap<>();
        termStats.put(stemmedPhrase, phraseStats);
        qDoc.setTermStats(termStats);
        
        // Generate and set snippet for phrase
        String snippet = generatePhraseSnippet(docId, phraseTerms);
        metadata.setSnippet(snippet);
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
        return Math.log((double) totalDocs / (1 + docsWithPhrase));
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
        nonStemmedQueryTerms = new ArrayList<>(tokens);
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

    private String selectBestSnippetForTerms(List<String> queryTerms, List<String> snippets) {
        if (snippets == null || snippets.isEmpty()) {
            return "No preview available";
        }
    
        if (snippets.size() == 1) {
            return highlightAllTerms(snippets.get(0));
        }
    
        if (queryTerms.size() > 1 && snippets.size() <= 5) {
            return joinSnippetsForDifferentTerms(queryTerms, snippets);
        }
    
        // Original scoring logic
        Map<String, Integer> snippetScores = new HashMap<>();
        for (String snippet : snippets) {
            int score = 0;
            String lowerSnippet = snippet.toLowerCase();
            for (String term : queryTerms) {
                if (lowerSnippet.contains(term.toLowerCase())) {
                    score++;
                    if (lowerSnippet.contains("<strong>" + term + "</strong>")) {
                        score += 2;
                    }
                }
            }
            snippetScores.put(snippet, score);
        }
        
        String bestSnippet = snippets.stream()
            .max(Comparator.comparingInt(snippetScores::get))
            .orElse(snippets.get(0));
        
        return highlightAllTerms(bestSnippet);
    }

    private String joinSnippetsForDifferentTerms(List<String> queryTerms, List<String> snippets) {
        Map<String, String> bestSnippetPerTerm = new HashMap<>();
    
        for (String term : queryTerms) {
            String bestForTerm = null;
            int bestScore = -1;
    
            for (String snippet : snippets) {
                int score = calculateSnippetScore(snippet, term);
                if (score > bestScore) {
                    bestScore = score;
                    bestForTerm = snippet;
                }
            }
    
            if (bestForTerm != null) {
                bestSnippetPerTerm.put(term, highlightAllTerms(bestForTerm));
            }
        }
    
        if (bestSnippetPerTerm.isEmpty()) {
            return highlightAllTerms(snippets.get(0));
        }
    
        List<String> distinctSnippets = new ArrayList<>(
            new LinkedHashSet<>(bestSnippetPerTerm.values())
        );
    
        if (distinctSnippets.size() > 3) {
            distinctSnippets = distinctSnippets.subList(0, 3);
        }
    
        return String.join(" ... | ... ", distinctSnippets);
    }

    private String highlightAllTerms(String snippet) {
        if (nonStemmedQueryTerms == null || nonStemmedQueryTerms.isEmpty()) {
            return snippet;
        }
    
        // Create pattern that matches any original term (case insensitive)
        String patternString = nonStemmedQueryTerms.stream()
            .map(Pattern::quote)
            .collect(Collectors.joining("|"));
        
        Pattern pattern = Pattern.compile("(?i)\\b(" + patternString + ")\\b");
        java.util.regex.Matcher matcher = pattern.matcher(snippet);
        
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            // Preserve original case from the document
            String matchedTerm = matcher.group(1);
            matcher.appendReplacement(sb, "<strong>" + matchedTerm + "</strong>");
        }
        matcher.appendTail(sb);
        
        return sb.toString();
    }
    private int calculateSnippetScore(String snippet, String term) {
        int score = 0;
        String lowerSnippet = snippet.toLowerCase();
        String lowerTerm = term.toLowerCase();
    
        if (lowerSnippet.contains(lowerTerm)) {
            // Score based on term position centrality
            int termPos = lowerSnippet.indexOf(lowerTerm);
            int snippetCenter = lowerSnippet.length() / 2;
            score += 100 - Math.min(100, Math.abs(termPos - snippetCenter));
    
            // Bonus if term is highlighted
            if (lowerSnippet.contains("<strong>" + term + "</strong>")) {
                score += 50;
            }
        }
        return score;
    }
    private String generatePhraseSnippet(String docId, List<String> phraseTerms) {
        try {
            // Get all snippets for each term in the phrase from their postings
            List<List<String>> allTermSnippets = new ArrayList<>();
            
            for (String term : phraseTerms) {
                Document wordDoc = wordIndexCollection.find(Filters.eq("word", term))
                    .projection(new Document("postings", 1))
                    .first();
                
                if (wordDoc != null) {
                    List<Document> postings = wordDoc.getList("postings", Document.class);
                    for (Document posting : postings) {
                        if (docId.equals(posting.getString("doc_id"))) {
                            List<String> snippets = TypeSafeUtil.safeStringList(posting.get("snippet"));
                            if (snippets != null && !snippets.isEmpty()) {
                                allTermSnippets.add(snippets);
                            }
                            break;
                        }
                    }
                }
            }
            
            // If we didn't get snippets for all terms, return a default
            if (allTermSnippets.size() != phraseTerms.size()) {
                return "Relevant content not available";
            }
            
            // Find snippets that contain all terms (or at least the first and last)
            List<String> candidateSnippets = new ArrayList<>();
            for (int i = 0; i < allTermSnippets.get(0).size(); i++) {
                String firstTermSnippet = allTermSnippets.get(0).get(i);
                
                // Check if this snippet appears in other terms' snippet lists
                boolean allContain = true;
                for (int j = 1; j < allTermSnippets.size(); j++) {
                    if (!allTermSnippets.get(j).contains(firstTermSnippet)) {
                        allContain = false;
                        break;
                    }
                }
                
                if (allContain) {
                    candidateSnippets.add(firstTermSnippet);
                }
            }
            
            // If no exact matches, try to find snippets that contain at least first and last term
            if (candidateSnippets.isEmpty()) {
                for (String snippet : allTermSnippets.get(0)) {
                    if (allTermSnippets.get(allTermSnippets.size() - 1).contains(snippet)) {
                        candidateSnippets.add(snippet);
                    }
                }
            }
            
            // If still no matches, just use the first term's first snippet
            if (candidateSnippets.isEmpty()) {
                candidateSnippets.add(allTermSnippets.get(0).get(0));
            }
            
            // Select the best snippet from candidates
            String bestSnippet = selectBestPhraseSnippet(candidateSnippets, phraseTerms);
            
            return highlightAllTerms(bestSnippet);
        } catch (Exception e) {
            e.printStackTrace();
            return "Error generating snippet";
        }
    }
    
    private String selectBestPhraseSnippet(List<String> snippets, List<String> phraseTerms) {
        if (snippets.size() == 1) {
            return snippets.get(0);
        }
        
        // Score snippets based on phrase term proximity and highlighting
        Map<String, Integer> snippetScores = new HashMap<>();
        String phrase = String.join(" ", phraseTerms);
        
        for (String snippet : snippets) {
            int score = 0;
            String lowerSnippet = snippet.toLowerCase();
            
            // Bonus if snippet contains the exact phrase
            if (lowerSnippet.contains(phrase.toLowerCase())) {
                score += 100;
            }
            
            // Check for all terms in order
            boolean allTermsInOrder = true;
            int lastPos = -1;
            for (String term : phraseTerms) {
                int pos = lowerSnippet.indexOf(term.toLowerCase());
                if (pos == -1) {
                    allTermsInOrder = false;
                    break;
                }
                if (pos < lastPos) {
                    allTermsInOrder = false;
                    break;
                }
                lastPos = pos;
            }
            
            if (allTermsInOrder) {
                score += 50;
            }
            
            // Bonus for highlighted terms
            for (String term : phraseTerms) {
                if (lowerSnippet.contains("<strong>" + term.toLowerCase() + "</strong>")) {
                    score += 20;
                }
            }
            
            snippetScores.put(snippet, score);
        }
        
        // Return highest scoring snippet
        return snippets.stream()
            .max(Comparator.comparingInt(snippetScores::get))
            .orElse(snippets.get(0));
    }
    
    private int[] convertTokenPositionsToCharOffsets(String content, int startTokenPos, int endTokenPos) {
        // Simple tokenization to match the indexer's token positions
        String[] tokens = content.split("\\s+");
        if (startTokenPos < 0 || endTokenPos >= tokens.length) {
            return null;
        }
    
        // Find the character offsets
        int charCount = 0;
        int startChar = -1;
        int endChar = -1;
        
        for (int i = 0; i < tokens.length; i++) {
            if (i == startTokenPos) {
                startChar = charCount;
            }
            if (i == endTokenPos) {
                endChar = charCount + tokens[i].length();
                break;
            }
            charCount += tokens[i].length() + 1; // +1 for the space
        }
    
        if (startChar == -1 || endChar == -1) {
            return null;
        }
    
        return new int[]{startChar, endChar};
    }
}

