package com.example.Indexer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.example.DatabaseConnection; // From com.example
import com.example.Crawler.PageHasher; // From com.example.Crawler
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.MongoException;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.*;
import com.example.util.TypeSafeUtil;

public class Indexer
{
    private final MongoCollection<Document> invertedIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final MongoCollection<Document> forwardIndexCollection;
    private final AtomicInteger docIdCounter = new AtomicInteger(0);
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;

    private static final double TITLE_WEIGHT = 3.0;
    private static final double HEADER_WEIGHT = 2.0;
    private static final double BODY_WEIGHT = 1.0;
    private static long totalDocs = 0;

    public Indexer()
    {
        this.invertedIndexCollection =
                DatabaseConnection.getDatabase().getCollection("inverted_index");
        this.forwardIndexCollection =
                DatabaseConnection.getDatabase().getCollection("forward_index");

        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();
        long count = documentsCollection.countDocuments();
        System.out.println("Total documents in collection: " + count);

        // Create indexes for fast retrieval
        invertedIndexCollection.createIndex(new Document("word", 1));
        documentsCollection.createIndex(new Document("doc_id", 1));
        documentsCollection.createIndex(new Document("url", 1));

        // Find highest existing document ID
        Document highestIdDoc =
                documentsCollection.find().sort(new Document("doc_id", -1)).limit(1).first();

        if (highestIdDoc != null)
        {
            String highestId = highestIdDoc.getString("doc_id");
            if (highestId != null && highestId.startsWith("doc_"))
            {
                try
                {
                    int id = Integer.parseInt(highestId.substring(4));
                    docIdCounter.set(id);
                    System.out.println("Initialized document counter to: " + id);
                }
                catch (NumberFormatException e)
                {
                    System.out.println("Using default document counter");
                }
            }
        }
    }

    public void indexDocuments(List<CrawledDoc> crawledDocs)
    {
        int processorCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newWorkStealingPool(processorCount);

        System.out.println("Starting indexing with " + processorCount + " threads...");

        for (CrawledDoc crawledDoc : crawledDocs)
        {
            executor.submit(() -> {
                try
                {
                    indexSingleDocument(crawledDoc);
                }
                catch (Exception e)
                {
                    System.err.println("Failed to index document: " + crawledDoc.getUrl() + " - "
                            + e.getMessage());
                }
            });
        }
        executor.shutdown();
        try
        {
            // Wait for all tasks to complete or timeout after 1 hour
            boolean finished = executor.awaitTermination(1, TimeUnit.HOURS);
            if (!finished)
                System.out.println("Indexing timed out before completion");
        }
        catch (InterruptedException e)
        {
            System.err.println("Indexing was interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
        System.out.println("Indexing completed. Total documents indexed: " + docIdCounter.get());
    }

    private void indexSingleDocument(CrawledDoc crawledDoc)
    {
        try
        {
            String url = crawledDoc.getUrl();
            String htmlContent = crawledDoc.getHtmlContent();
            String title = crawledDoc.getTitle();

            // Add null checks for critical data
            if (htmlContent == null || htmlContent.isEmpty())
            {
                System.err.println("Empty HTML content for: " + url);
                return;
            }

            String contentHash = PageHasher.generateHash(htmlContent);
            long timestamp = System.currentTimeMillis();

            // Check for duplicates
            Document existingDoc =
                    documentsCollection.find(Filters.eq("content_hash", contentHash)).first();
            if (existingDoc != null)
            {
                System.out.println("Document already indexed, skipping: " + url);
                return;
            }

            // Parse HTML content with error handling
            org.jsoup.nodes.Document doc;
            try
            {
                doc = Jsoup.parse(htmlContent, url);
            }
            catch (Exception e)
            {
                System.err.println("Failed to parse HTML for: " + url + " - " + e.getMessage());
                return;
            }

            String docId = "doc_" + docIdCounter.incrementAndGet();

            // Extract words and compute statistics
            int globalPosition = 0;
            Map<String, WordStats> wordStats = new HashMap<>();
            int totalWords = 0;

            // Process title with null check
            if (title != null && !title.isEmpty())
            {
                totalWords += processText(title, TITLE_WEIGHT, wordStats, globalPosition);
                globalPosition = totalWords;
            }
            else
            {
                System.out.println("Warning: No title for document: " + url);
                // Try to extract title from HTML if missing
                title = doc.title();
            }

            // Process headers (h1–h6)
            for (int i = 1; i <= 6; i++)
            {
                Elements headers = doc.select("h" + i);
                for (Element header : headers)
                {
                    totalWords +=
                            processText(header.text(), HEADER_WEIGHT, wordStats, globalPosition);
                    globalPosition = totalWords;
                }
            }

            // Process body text with null check
            if (doc.body() != null)
            {
                totalWords +=
                        processText(doc.body().text(), BODY_WEIGHT, wordStats, globalPosition);
            }

            // Skip documents with no extracted content
            if (totalWords == 0 || wordStats.isEmpty())
            {
                System.out.println("Skipping document with no content: " + url);
                return;
            }

            // Save document metadata
            Document docEntry = new Document().append("doc_id", docId)
                    .append("content_hash", contentHash).append("url", url).append("title", title);

            documentsCollection.insertOne(docEntry);

            // Update word index with TF
            updateWordIndex(docId, url, title, wordStats, totalWords, timestamp);

            System.out.println("Indexed document: " + url + " (ID: " + docId + ")");

            if (crawledDoc.getMongoId() != null)
            {
                MongoCollection<Document> pagesCollection =
                        DatabaseConnection.getDatabase().getCollection("clean_pages");
                pagesCollection.updateOne(
                        Filters.eq("_id", new org.bson.types.ObjectId(crawledDoc.getMongoId())),
                        new Document("$set",
                                new Document("indexed", true).append("doc_id", docId)));
            }
        }
        catch (Exception e)
        {
            // Provide more detailed error information
            System.err.println("Failed to index document: " + crawledDoc.getUrl() + " - "
                    + e.getClass().getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private int processText(String text, double weight, Map<String, WordStats> wordStats,
            int startPosition)
    {
        if (text == null || text.trim().isEmpty())
            return 0;

        // Tokenize the text
        List<String> tokens = tokenizer.tokenize(text);
        if (tokens.isEmpty())
            return 0;

        // Remove stop words
        tokens = stopWords.removeStopWords(tokens);
        if (tokens.isEmpty())
            return 0;

        // Stem tokens and update word statistics
        int wordCount = 0;
        for (String token : tokens)
        {
            try
            {
                String stemmedWord = stemmer.stem(token);
                if (stemmedWord == null || stemmedWord.isEmpty())
                    continue;

                int position = startPosition + wordCount;
                wordStats.computeIfAbsent(stemmedWord, k -> new WordStats()).addOccurrence(weight,
                        position);
                wordCount++;
            }
            catch (Exception e)
            {
                // Log error but continue processing other tokens
                System.err.println("Error processing token '" + token + "': " + e.getMessage());
            }
        }

        return wordCount;
    }

    private void updateWordIndex(String docId, String url, String title,
            Map<String, WordStats> wordStats, int totalWords, long timestamp)
    {

        // Process title the same way as content to get stemmed words
        Set<String> stemmedTitleWords = new HashSet<>();
        if (title != null && !title.isEmpty())
        {
            Map<String, WordStats> titleWordStats = new HashMap<>();
            processText(title, 1.0, titleWordStats, 0);
            stemmedTitleWords.addAll(titleWordStats.keySet());
        }

        List<WriteModel<Document>> bulkOperations = new ArrayList<>();

        List<String> wordsForDocument = new ArrayList<>(wordStats.keySet());
        forwardIndexCollection
                .insertOne(new Document("doc_id", docId).append("words", wordsForDocument));

        for (Map.Entry<String, WordStats> entry : wordStats.entrySet())
        {
            String word = entry.getKey();
            WordStats stats = entry.getValue();
            boolean inTitle = stemmedTitleWords.contains(word);

            // Calculate TF: frequency / total_words
            double tf = totalWords > 0 ? (double) stats.getFrequency() / totalWords : 0.0;

            Document posting = new Document().append("doc_id", docId).append("url", url)
                    .append("in_title", inTitle).append("frequency", stats.getFrequency())
                    .append("tf", tf).append("importance_score", stats.getImportanceScore())
                    .append("length", totalWords).append("timestamp", timestamp)
                    .append("positions", stats.getPositions());

            // Instead of immediate update, add to bulk operations
            bulkOperations
                    .add(new UpdateOneModel<>(Filters.eq("word", word),
                            new Document("$push", new Document("postings", posting)).append("$inc",
                                    new Document("doc_count", 1)),
                            new UpdateOptions().upsert(true)));
        }

        // Execute the bulk operations if not empty
        if (!bulkOperations.isEmpty())
        {
            invertedIndexCollection.bulkWrite(bulkOperations,
                    new BulkWriteOptions().ordered(false));
        }
    }

    private double computeIdf(int docsWithTerm)
    {
        double idf = Math.log((double) totalDocs / (1 + docsWithTerm));
        return idf;
    }

    private void computeAndStoreIdfValues()
    {
        // Get the actual total document count from the database
        totalDocs = documentsCollection.countDocuments();
        System.out.println("Computing IDF values based on " + totalDocs + " total documents");

        AtomicBoolean connectionActive = new AtomicBoolean(true);
        int processorCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);
        AtomicInteger completedCount = new AtomicInteger(0);

        // Count total words to process
        long totalWords = invertedIndexCollection.countDocuments();
        System.out.println("Starting IDF calculation with " + processorCount + " threads for "
                + totalWords + " words...");

        // Create concurrent queue for bulk operations
        ConcurrentLinkedQueue<WriteModel<Document>> bulkOperations = new ConcurrentLinkedQueue<>();

        // Create a thread to periodically process bulk operations
        Thread bulkWriter = new Thread(() -> {
            List<WriteModel<Document>> batch = new ArrayList<>();
            while ((connectionActive.get() && !executor.isTerminated())
                    || !bulkOperations.isEmpty())
            {
                try
                {
                    // Wait a bit before each check
                    Thread.sleep(500);

                    // Move items from queue to batch
                    while (!bulkOperations.isEmpty() && batch.size() < 1000)
                    {
                        WriteModel<Document> op = bulkOperations.poll();
                        if (op != null)
                        {
                            batch.add(op);
                        }
                    }

                    // Process batch if not empty
                    if (!batch.isEmpty())
                    {
                        try
                        {
                            invertedIndexCollection.bulkWrite(batch,
                                    new BulkWriteOptions().ordered(false));
                            System.out.println("Bulk processed " + batch.size() + " IDF updates");
                        }
                        catch (MongoException e)
                        {
                            if (e.getMessage().contains("no longer available"))
                            {
                                System.err.println(
                                        "MongoDB connection lost. Stopping bulk operations.");
                                connectionActive.set(false);
                                break; // Exit the processing loop
                            }
                            else
                            {
                                System.err.println("Error in IDF bulk write: " + e.getMessage());
                                Thread.sleep(2000); // Wait before retry
                            }
                        }
                        batch.clear();
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Final processing of any remaining operations
            if (!bulkOperations.isEmpty())
            {
                List<WriteModel<Document>> finalBatch = new ArrayList<>();
                while (!bulkOperations.isEmpty())
                {
                    WriteModel<Document> op = bulkOperations.poll();
                    if (op != null)
                    {
                        finalBatch.add(op);
                    }
                }
                try
                {
                    invertedIndexCollection.bulkWrite(finalBatch,
                            new BulkWriteOptions().ordered(false));
                    System.out
                            .println("Final bulk processed " + finalBatch.size() + " IDF updates");
                }
                catch (Exception e)
                {
                    System.err.println("Error in final IDF bulk write: " + e.getMessage());
                }
            }
        });
        bulkWriter.start();

        // Process all words with thread pool
        for (Document wordEntry : invertedIndexCollection.find())
        {
            executor.submit(() -> {
                try
                {
                    String word = wordEntry.getString("word");
                    Integer docCount = wordEntry.getInteger("doc_count");
                    if (docCount == null)
                    {
                        System.err.println("Warning: Missing doc_count for word: " + word);
                        return;
                    }

                    double idf = computeIdf(docCount);

                    // Instead of immediate update, add to bulk operations
                    bulkOperations.add(new UpdateOneModel<>(Filters.eq("word", word),
                            new Document("$set", new Document("idf", idf))));

                    int completed = completedCount.incrementAndGet();
                    if (completed % 1000 == 0 || completed == totalWords)
                        System.out.println(
                                "IDF Progress: " + completed + "/" + totalWords + " words");
                }
                catch (Exception e)
                {
                    System.err.println("Failed to calculate IDF for word: "
                            + wordEntry.getString("word") + " - " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        try
        {
            // Wait for all tasks to complete or timeout after 1 hour
            boolean finished = executor.awaitTermination(1, TimeUnit.HOURS);
            if (!finished)
            {
                System.out.println("IDF calculation timed out before completion");
            }
        }
        catch (InterruptedException e)
        {
            System.err.println("IDF calculation was interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }

        // Wait for bulk writer to finish
        try
        {
            bulkWriter.join(5000);
        }
        catch (InterruptedException e)
        {
            System.err.println("Bulk writer thread interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }

        System.out.println("IDF values calculated and stored for all indexed words.");
    }

    // Update to support incremental mode
    public List<CrawledDoc> fetchCrawledDocumentsBatch(int skip, int limit)
    {
        MongoCollection<Document> pagesCollection =
                DatabaseConnection.getDatabase().getCollection("pages");

        List<CrawledDoc> documents = Collections.synchronizedList(new ArrayList<>());
        List<String> docIdsToClean = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errorCount = new AtomicInteger(0);

        // Create query based on incremental mode
        Document query = new Document("indexed", false);

        int processorCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newWorkStealingPool(processorCount);

        // Find documents with pagination
        for (Document doc : pagesCollection.find(query).skip(skip).limit(limit))
        {
            executor.submit(() -> {
                try
                {
                    String url = doc.getString("url");
                    String content = doc.getString("content");
                    String title = doc.getString("title");

                    if (content == null || content.isEmpty())
                    {
                        errorCount.incrementAndGet();
                        return;
                    }

                    CrawledDoc crawledDoc = new CrawledDoc(url, title, content);

                    // Store MongoDB ID for marking as indexed later
                    crawledDoc.setMongoId(doc.getObjectId("_id").toString());

                    // Check if this document ID exists in the forward index
                    if (doc.containsKey("doc_id"))
                    {
                        String existingDocId = doc.getString("doc_id");

                        // If it exists in forward index, it was actually indexed before
                        Document forwardIndexEntry = forwardIndexCollection
                                .find(Filters.eq("doc_id", existingDocId)).first();

                        if (forwardIndexEntry != null)
                        {
                            crawledDoc.setDocId(existingDocId);
                            docIdsToClean.add(existingDocId);
                        }
                    }

                    documents.add(crawledDoc);
                }
                catch (Exception e)
                {
                    errorCount.incrementAndGet();
                }
            });
        }

        executor.shutdown();
        try
        {
            executor.awaitTermination(5, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        // Clean existing index entries efficiently
        if (!docIdsToClean.isEmpty())
        {
            cleanIndexEntriesForDocuments(docIdsToClean);
        }

        return documents;
    }

    /**
     * Efficiently clean index entries using forward index
     * 
     * @param docIds List of document IDs to clean from the index
     */
    private void cleanIndexEntriesForDocuments(List<String> docIds)
    {
        if (docIds.isEmpty())
            return;

        try
        {
            System.out.println("Cleaning index entries for " + docIds.size() + " documents");

            // First, find all words affected by these documents
            Set<String> wordsToUpdate = new HashSet<>();

            // Use the forward index to efficiently find affected words
            for (Document mapping : forwardIndexCollection.find(Filters.in("doc_id", docIds)))
            {
                List<String> words = TypeSafeUtil.safeStringList(mapping.get("words"));
                if (!words.isEmpty())
                {
                    wordsToUpdate.addAll(words);
                }
            }

            System.out.println("Found " + wordsToUpdate.size() + " words to update");

            // Process words in batches for better performance
            List<WriteModel<Document>> bulkOperations = new ArrayList<>();

            for (String word : wordsToUpdate)
            {
                // Pull all matching document IDs from the postings array
                bulkOperations.add(new UpdateOneModel<>(Filters.eq("word", word),
                        new Document("$pull", new Document("postings",
                                new Document("doc_id", new Document("$in", docIds))))));

                // Execute in batches of 1000 operations
                if (bulkOperations.size() >= 1000)
                {
                    invertedIndexCollection.bulkWrite(bulkOperations,
                            new BulkWriteOptions().ordered(false));
                    bulkOperations.clear();
                }
            }

            // Execute any remaining operations
            if (!bulkOperations.isEmpty())
            {
                invertedIndexCollection.bulkWrite(bulkOperations,
                        new BulkWriteOptions().ordered(false));
            }

            // Update doc_count for affected words
            for (String word : wordsToUpdate)
            {
                Document wordDoc = invertedIndexCollection.find(Filters.eq("word", word)).first();
                if (wordDoc != null)
                {
                    List<Document> postings =
                            TypeSafeUtil.safeDocumentList(wordDoc.get("postings"));

                    // Update doc_count to match actual number of postings
                    if (postings.isEmpty())
                    {
                        // No more documents contain this word - remove it
                        invertedIndexCollection.deleteOne(Filters.eq("word", word));
                    }
                    else
                    {
                        invertedIndexCollection.updateOne(Filters.eq("word", word),
                                new Document("$set", new Document("doc_count", postings.size())));
                    }
                }
            }

            // Remove from documents collection
            documentsCollection.deleteMany(Filters.in("doc_id", docIds));

            // Remove the mappings
            forwardIndexCollection.deleteMany(Filters.in("doc_id", docIds));

            System.out.println("Completed cleaning index entries");
        }
        catch (Exception e)
        {
            System.err.println("Error cleaning index entries: " + e.getMessage());
        }
    }

    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        Indexer indexer = new Indexer();

        try
        {
            // Process documents in batches to avoid memory issues
            MongoCollection<Document> pagesCollection =
                    DatabaseConnection.getDatabase().getCollection("pages");

            // Get count based on mode
            long totalDocs = pagesCollection.countDocuments();
            int batchSize = 500; // Smaller batches to control memory usage

            for (int skip = 0; skip < totalDocs; skip += batchSize)
            {
                System.out.println("Processing batch: " + (skip / batchSize + 1) + " of "
                        + (int) Math.ceil((double) totalDocs / batchSize));

                // Pass incremental mode flag
                List<CrawledDoc> batch = indexer.fetchCrawledDocumentsBatch(skip, batchSize);
                System.out.println("Retrieved " + batch.size() + " documents to index");

                if (!batch.isEmpty())
                {
                    indexer.indexDocuments(batch);
                }

                // Suggest garbage collection between batches
                batch.clear();
                System.gc();
            }

            // Ensure IDF calculations complete before closing connection
            totalDocs = pagesCollection.countDocuments();
            indexer.computeAndStoreIdfValues();
        }
        catch (Exception e)
        {
            System.err.println("Indexing error: " + e.getMessage());
            e.printStackTrace();
        } finally
        {
            // Give any in-progress operations a chance to complete
            try
            {
                System.out.println(
                        "Allowing final operations to complete before closing connection...");
                Thread.sleep(5000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }

            // Now close the connection
            DatabaseConnection.closeConnection();

            long endTime = System.currentTimeMillis();
            System.out.println("Total indexing time: " + (endTime - startTime) / 1000 + " seconds");
        }
    }

}
