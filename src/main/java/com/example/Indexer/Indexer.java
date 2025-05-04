package com.example.Indexer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Collectors;
import java.util.*;
import com.example.DatabaseConnection;
import com.example.Crawler.PageHasher;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.*;
import org.bson.types.ObjectId;
import org.jsoup.*;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Indexer {
    private final MongoCollection<Document> invertedIndexCollection;
    private final MongoCollection<Document> forwardIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final MongoCollection<Document> pagesCollection;
    private final AtomicInteger docIdCounter = new AtomicInteger(0);
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;

    // Weighting factors
    private static final double TITLE_WEIGHT = 3.0;
    private static final double HEADER_WEIGHT = 2.0;
    private static final double BODY_WEIGHT = 1.0;
    private static final double URL_WEIGHT = 0.5;
    private static final double META_WEIGHT = 1.5;

    // Configuration
    private static final int BATCH_SIZE = 500;
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_RETRIES = 3;
    private static final int SNIPPET_LENGTH = 150;

    public Indexer() {
        MongoDatabase database = DatabaseConnection.getDatabase();
        
        this.invertedIndexCollection = database.getCollection("inverted_index");
        this.forwardIndexCollection = database.getCollection("forward_index");
        this.documentsCollection = database.getCollection("documents");
        this.pagesCollection = database.getCollection("pages");
        
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();

        createIndexes();
        initializeDocIdCounter();
    }

    private void createIndexes() {
        try {
            // Inverted Index
            invertedIndexCollection.createIndex(Indexes.ascending("word"));
            invertedIndexCollection.createIndex(Indexes.ascending("postings.doc_id"));
            invertedIndexCollection.createIndex(Indexes.ascending("postings.url"));
            
            // Forward Index
            forwardIndexCollection.createIndex(Indexes.ascending("doc_id"));
            forwardIndexCollection.createIndex(Indexes.ascending("words"));
            
            // Documents
            documentsCollection.createIndex(Indexes.ascending("doc_id"), 
                new IndexOptions().unique(true));
            documentsCollection.createIndex(Indexes.ascending("url"));
            documentsCollection.createIndex(Indexes.text("title"));
            
            // Pages (ensure crawler's indexes are complemented)
            pagesCollection.createIndex(Indexes.ascending("indexed"));
            pagesCollection.createIndex(Indexes.ascending("doc_id"));
            pagesCollection.createIndex(Indexes.ascending("timestamp"));
            
            System.out.println("All necessary indexes created/verified");
        } catch (Exception e) {
            System.err.println("Index creation failed: " + e.getMessage());
        }
    }

    private void initializeDocIdCounter() {
        Document lastDoc = documentsCollection.find()
            .sort(Sorts.descending("doc_id"))
            .limit(1)
            .first();
        
        if (lastDoc != null) {
            String lastId = lastDoc.getString("doc_id");
            if (lastId != null && lastId.startsWith("doc_")) {
                int id = Integer.parseInt(lastId.substring(4));
                docIdCounter.set(id + 1);
            }
        }
        System.out.println("Document counter initialized to: " + docIdCounter.get());
    }

    public void startIndexing() {
        long totalUnindexed = pagesCollection.countDocuments(
            Filters.or(
                Filters.eq("indexed", false),
                Filters.exists("indexed", false)
            )
        );
        
        System.out.println("Starting indexing of " + totalUnindexed + " unindexed pages");
        
        int batches = (int) Math.ceil((double) totalUnindexed / BATCH_SIZE);
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
        
        for (int i = 0; i < batches; i++) {
            final int batchNumber = i;
            executor.submit(() -> processBatch(batchNumber * BATCH_SIZE, BATCH_SIZE));
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.out.println("Indexing timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        computeAndStoreIdfValues();
        System.out.println("Indexing completed successfully");
    }

    private void processBatch(int skip, int limit) {
        List<Document> batch = pagesCollection.find(
                Filters.or(
                    Filters.eq("indexed", false),
                    Filters.exists("indexed", false)
                ))
            .skip(skip)
            .limit(limit)
            .into(new ArrayList<>());
        
        System.out.println("Processing batch with " + batch.size() + " documents");
        
        List<WriteModel<Document>> invertedIndexUpdates = new ArrayList<>();
        List<Document> forwardIndexDocuments = new ArrayList<>();
        List<Document> documentMetadata = new ArrayList<>();
        List<WriteModel<Document>> pageUpdates = new ArrayList<>();
        
        for (Document page : batch) {
            try {
                String url = page.getString("url");
                String content = page.getString("content");
                String title = page.getString("title");
                Double popularity = page.getDouble("popularity");
                ObjectId pageId = page.getObjectId("_id");
                
                if (content == null || content.isEmpty()) {
                    continue;
                }
                
                // Check for duplicates
                String contentHash = PageHasher.generateHash(content);
                if (documentsCollection.countDocuments(Filters.eq("content_hash", contentHash)) > 0) {
                    markAsIndexed(pageId);
                    continue;
                }
                
                // Parse and process document
                org.jsoup.nodes.Document doc = Jsoup.parse(content, url);
                String docId = "doc_" + docIdCounter.getAndIncrement();
                
                // Process document sections
                Map<String, WordStats> wordStats = new HashMap<>();
                int totalWords = processDocumentSections(doc, title, wordStats);
                
                if (totalWords == 0) {
                    continue;
                }
                
                // Prepare documents for bulk insertion
                prepareDocuments(
                    docId, url, title, contentHash, popularity, 
                    wordStats, totalWords,
                    invertedIndexUpdates, forwardIndexDocuments, 
                    documentMetadata, pageUpdates, pageId
                );
                
            } catch (Exception e) {
                System.err.println("Error processing document: " + e.getMessage());
            }
        }
        
        // Execute bulk operations
        executeBulkOperations(
            invertedIndexUpdates, forwardIndexDocuments, 
            documentMetadata, pageUpdates
        );
    }

    private int processDocumentSections(org.jsoup.nodes.Document doc, String title, 
            Map<String, WordStats> wordStats) {
        int totalWords = 0;
        int globalPosition = 0;
        
        // Process title
        if (title != null && !title.isEmpty()) {
            totalWords += processText(title, TITLE_WEIGHT, wordStats, globalPosition);
            globalPosition = totalWords;
        }
        
        // Process meta tags
        Elements metaTags = doc.select("meta[name=description], meta[name=keywords]");
        for (Element meta : metaTags) {
            String content = meta.attr("content");
            if (!content.isEmpty()) {
                totalWords += processText(content, META_WEIGHT, wordStats, globalPosition);
                globalPosition = totalWords;
            }
        }
        
        // Process headers
        for (int i = 1; i <= 6; i++) {
            Elements headers = doc.select("h" + i);
            for (Element header : headers) {
                totalWords += processText(header.text(), HEADER_WEIGHT, wordStats, globalPosition);
                globalPosition = totalWords;
            }
        }
        
        // Process body
        if (doc.body() != null) {
            totalWords += processText(doc.body().text(), BODY_WEIGHT, wordStats, globalPosition);
        }
        
        // Process URL (split and process as tokens)
        String url = doc.baseUri();
        if (url != null) {
            totalWords += processText(url.replaceAll("[^a-zA-Z0-9]", " "), URL_WEIGHT, wordStats, globalPosition);
        }
        
        return totalWords;
    }

    private int processText(String text, double weight, Map<String, WordStats> wordStats, int startPosition) {
        if (text == null || text.trim().isEmpty()) return 0;
        
        List<String> tokens = tokenizer.tokenize(text);
        tokens = stopWords.removeStopWords(tokens);
        
        int wordCount = 0;
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            String stemmed = stemmer.stem(token);
            
            if (stemmed == null || stemmed.isEmpty()) continue;
            
            int position = startPosition + wordCount;
            String snippet = extractContextSnippet(text, token);
            
            wordStats.computeIfAbsent(stemmed, k -> new WordStats())
                .addOccurrence(weight, position, snippet);
            
            wordCount++;
        }
        return wordCount;
    }

    private String extractContextSnippet(String text, String token) {
        int tokenPos = text.toLowerCase().indexOf(token.toLowerCase());
        if (tokenPos == -1) return text.substring(0, Math.min(text.length(), SNIPPET_LENGTH));
        
        int start = Math.max(0, tokenPos - SNIPPET_LENGTH/2);
        int end = Math.min(text.length(), tokenPos + token.length() + SNIPPET_LENGTH/2);
        
        // Adjust to nearest word boundaries
        while (start > 0 && !Character.isWhitespace(text.charAt(start))) start--;
        while (end < text.length() && !Character.isWhitespace(text.charAt(end))) end++;
        
        String snippet = text.substring(start, end);
        if (start > 0) snippet = "..." + snippet;
        if (end < text.length()) snippet = snippet + "...";
        
        return snippet;
    }

    private void prepareDocuments(
        String docId, String url, String title, String contentHash, Double popularity,
        Map<String, WordStats> wordStats, int totalWords,
        List<WriteModel<Document>> invertedIndexUpdates,
        List<Document> forwardIndexDocuments,
        List<Document> documentMetadata,
        List<WriteModel<Document>> pageUpdates,
        ObjectId pageId
    ) {
        // Prepare inverted index updates
        Set<String> titleWords = extractTitleWords(title);
        List<String> words = new ArrayList<>(wordStats.keySet());
        
        for (Map.Entry<String, WordStats> entry : wordStats.entrySet()) {
            String word = entry.getKey();
            WordStats stats = entry.getValue();
            
            double tf = (double) stats.getFrequency() / totalWords;
            List<String> snippets = selectBestSnippets(stats.getContextSnippets().values(), 3);
            
            Document posting = new Document()
                .append("doc_id", docId)
                .append("url", url)
                .append("title", title)
                .append("in_title", titleWords.contains(word))
                .append("frequency", stats.getFrequency())
                .append("tf", tf)
                .append("positions", stats.getPositions())
                .append("popularity", popularity)
                .append("snippets", snippets)
                .append("timestamp", System.currentTimeMillis());
            
            invertedIndexUpdates.add(
                new UpdateOneModel<>(
                    Filters.eq("word", word),
                    new Document("$push", new Document("postings", posting))
                        .append("$inc", new Document("doc_count", 1)),
                    new UpdateOptions().upsert(true)
                )
            );
        }
        
        // Prepare forward index document
        forwardIndexDocuments.add(
            new Document()
                .append("doc_id", docId)
                .append("words", words)
                .append("word_count", totalWords)
        );
        
        // Prepare document metadata
        documentMetadata.add(
            new Document()
                .append("doc_id", docId)
                .append("url", url)
                .append("title", title)
                .append("content_hash", contentHash)
                .append("popularity", popularity)
                .append("indexed_at", new Date())
                .append("word_count", totalWords)
        );
        
        // Prepare page update
        pageUpdates.add(
            new UpdateOneModel<>(
                Filters.eq("_id", pageId),
                Updates.combine(
                    Updates.set("indexed", true),
                    Updates.set("doc_id", docId),
                    Updates.set("indexed_at", new Date())
                )
            )
        );
    }

    private Set<String> extractTitleWords(String title) {
        if (title == null) return Collections.emptySet();
        
        Set<String> titleWords = new HashSet<>();
        List<String> tokens = tokenizer.tokenize(title);
        tokens = stopWords.removeStopWords(tokens);
        
        for (String token : tokens) {
            String stemmed = stemmer.stem(token);
            if (stemmed != null && !stemmed.isEmpty()) {
                titleWords.add(stemmed);
            }
        }
        
        return titleWords;
    }

    private List<String> selectBestSnippets(Collection<String> snippets, int maxCount) {
        if (snippets.size() <= maxCount) return new ArrayList<>(snippets);
        
        return snippets.stream()
            .sorted((a, b) -> Integer.compare(b.length(), a.length())) // Prefer longer snippets
            .limit(maxCount)
            .collect(Collectors.toList());
    }

    private void executeBulkOperations(
        List<WriteModel<Document>> invertedIndexUpdates,
        List<Document> forwardIndexDocuments,
        List<Document> documentMetadata,
        List<WriteModel<Document>> pageUpdates
    ) {
        // Execute in transaction for atomicity
        try (ClientSession session = DatabaseConnection.getMongoClient().startSession()) {
            session.startTransaction();
            
            try {
                if (!invertedIndexUpdates.isEmpty()) {
                    invertedIndexCollection.bulkWrite(session, invertedIndexUpdates, 
                        new BulkWriteOptions().ordered(false));
                }
                
                if (!forwardIndexDocuments.isEmpty()) {
                    forwardIndexCollection.insertMany(session, forwardIndexDocuments);
                }
                
                if (!documentMetadata.isEmpty()) {
                    documentsCollection.insertMany(session, documentMetadata);
                }
                
                if (!pageUpdates.isEmpty()) {
                    pagesCollection.bulkWrite(session, pageUpdates, 
                        new BulkWriteOptions().ordered(false));
                }
                
                session.commitTransaction();
                System.out.println("Batch processed: " + documentMetadata.size() + " documents");
            } catch (Exception e) {
                session.abortTransaction();
                System.err.println("Batch failed: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Session error: " + e.getMessage());
        }
    }

    private void markAsIndexed(ObjectId pageId) {
        pagesCollection.updateOne(
            Filters.eq("_id", pageId),
            Updates.set("indexed", true)
        );
    }

    private void computeAndStoreIdfValues() {
        long totalDocs = documentsCollection.countDocuments();
        System.out.println("Computing IDF values for " + totalDocs + " documents");
        
        MongoCursor<Document> cursor = invertedIndexCollection.find().iterator();
        List<WriteModel<Document>> updates = new ArrayList<>();
        int batchSize = 1000;
        
        try {
            while (cursor.hasNext()) {
                Document wordEntry = cursor.next();
                String word = wordEntry.getString("word");
                int docCount = wordEntry.getInteger("doc_count", 1);
                
                double idf = Math.log((double) totalDocs / docCount);
                
                updates.add(
                    new UpdateOneModel<>(
                        Filters.eq("word", word),
                        Updates.set("idf", idf)
                    )
                );
                
                if (updates.size() >= batchSize) {
                    invertedIndexCollection.bulkWrite(updates, 
                        new BulkWriteOptions().ordered(false));
                    updates.clear();
                    System.out.println("Processed " + batchSize + " IDF updates");
                }
            }
            
            if (!updates.isEmpty()) {
                invertedIndexCollection.bulkWrite(updates, 
                    new BulkWriteOptions().ordered(false));
            }
        } finally {
            cursor.close();
        }
        
        System.out.println("IDF computation completed");
    }

    public static void main(String[] args) {
        Indexer indexer = new Indexer();
        indexer.startIndexing();
        DatabaseConnection.closeConnection();
    }
}