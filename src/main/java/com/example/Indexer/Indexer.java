package com.example.Indexer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
    private final MongoCollection<Document> pagesCollection;
    private final AtomicInteger docIdCounter = new AtomicInteger(0);
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;

    private static final double TITLE_WEIGHT = 3.0;
    private static final double HEADER_WEIGHT = 2.0;
    private static final double BODY_WEIGHT = 1.0;
    private static long totalDocs = 0;

    // Constructor initializes collections and indexes
    public Indexer()
    {
        this.invertedIndexCollection =
                DatabaseConnection.getDatabase().getCollection("inverted_index");
        this.forwardIndexCollection =
                DatabaseConnection.getDatabase().getCollection("forward_index");
        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.pagesCollection = DatabaseConnection.getDatabase().getCollection("pages");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();
        long count = pagesCollection.countDocuments();
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
                int id = Integer.parseInt(highestId.substring(4));
                docIdCounter.set(id);
            }
        }
    }

    // Index documents in parallel using ExecutorService
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
            // Wait for all tasks to complete or timeout after 2 hours
            boolean finished = executor.awaitTermination(2, TimeUnit.HOURS);
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

    // Index a single document
    private void indexSingleDocument(CrawledDoc crawledDoc)
    {
        try
        {
            String url = crawledDoc.getUrl();
            String htmlContent = crawledDoc.getHtmlContent();
            String title = crawledDoc.getTitle();
            double popularity = crawledDoc.getPopularity();

            System.out.println("==============================================");
            System.out.println("popularity: " + popularity);
            System.out.println("==============================================");

            // Check for null or empty content
            if (htmlContent == null || htmlContent.isEmpty())
                return;

            // Check for duplicates
            String contentHash = PageHasher.generateHash(htmlContent);
            Document existingDoc =
                    documentsCollection.find(Filters.eq("content_hash", contentHash)).first();
            if (existingDoc != null)
                return;

            // Parse HTML content
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
                totalWords +=
                        processText(doc.body().text(), BODY_WEIGHT, wordStats, globalPosition);

            // Skip documents with no extracted content
            if (totalWords == 0 || wordStats.isEmpty())
                return;

            // Save document metadata
            Document docEntry = new Document().append("doc_id", docId)

                    

            documentsCollection.insertOne(docEntry);

            // Update word index with TF
            long timestamp = System.currentTimeMillis();
            updateWordIndex(docId, url, title, wordStats, totalWords, timestamp, popularity);
            System.out.println("Indexed document: " + url + " (ID: " + docId + ")");

            // Update forward index and mark page as indexed
            if (crawledDoc.getMongoId() != null)
            {
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

    // Process text to extract words
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
        List<String> filteredTokens = stopWords.removeStopWords(tokens);
        if (filteredTokens.isEmpty())
            return 0;

        // Process tokens
        int wordCount = 0;
        for (int i = 0; i < filteredTokens.size(); i++)
        {
            try
            {
                String token = filteredTokens.get(i);
                String stemmedWord = stemmer.stem(token);
                if (stemmedWord == null || stemmedWord.isEmpty())
                    continue;
                int position = startPosition + wordCount;

                // Generate snippet from original text
                String contextSnippet = extractContextSnippet(text, token);

                // Add the occurrence with context
                wordStats.computeIfAbsent(stemmedWord, k -> new WordStats()).addOccurrence(weight,
                        position, contextSnippet);

                wordCount++;
            }
            catch (Exception e)
            {
                System.err.println("Error processing token '" + filteredTokens.get(i) + "': "
                        + e.getMessage());
            }
        }
        return wordCount;
    }

    // Helper method to extract context around a token
    private String extractContextSnippet(String originalText, String token)
    {
        // Find the token in the original text
        int tokenIndex = originalText.toLowerCase().indexOf(token.toLowerCase());
        if (tokenIndex == -1)
        {
            // If exact match not found, return a reasonable substring
            return originalText.length() <= 150 ? originalText
                    : originalText.substring(0, 150) + "...";
        }

        // Determine the snippet boundaries (approximately 100 chars before and after)
        int snippetStart = Math.max(0, tokenIndex - 150);
        int snippetEnd = Math.min(originalText.length(), tokenIndex + token.length() + 150);

        // If we're starting mid-sentence, try to find the beginning of the sentence or phrase
        if (snippetStart > 0)
        {
            int periodPos = originalText.lastIndexOf(". ", tokenIndex);
            if (periodPos != -1 && periodPos > tokenIndex - 150)
                snippetStart = periodPos + 2; // Start after the period and space
        }

        // If we're ending mid-sentence, try to find the end of the sentence
        if (snippetEnd < originalText.length())
        {
            int periodPos = originalText.indexOf(".", tokenIndex);
            if (periodPos != -1 && periodPos < tokenIndex + 150)
                snippetEnd = Math.min(periodPos + 1, originalText.length());
        }

        // Extract the snippet
        String before = originalText.substring(snippetStart, tokenIndex);
        String highlight = originalText.substring(tokenIndex, tokenIndex + token.length());
        String after = originalText.substring(tokenIndex + token.length(), snippetEnd);

        // Add ellipsis where needed
        String prefix = snippetStart > 0 ? "..." : "";
        String suffix = snippetEnd < originalText.length() ? "..." : "";

        // Return the formatted snippet with highlighted term
        return prefix + before + "<strong>" + highlight + "</strong>" + after + suffix;
    }

    // Update the inverted index with word statistics
    private void updateWordIndex(String docId, String url, String title,
            Map<String, WordStats> wordStats, int totalWords, long timestamp, double popularity)

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

        // Update the inverted index with word statistics
        for (Map.Entry<String, WordStats> entry : wordStats.entrySet())
        {
            String word = entry.getKey();
            WordStats stats = entry.getValue();
            boolean inTitle = stemmedTitleWords.contains(word);
            List<String> topSnippets = new ArrayList<>(stats.getContextSnippets().values());
            List<String> snippets = selectBestSnippets(topSnippets, 3);


            // Calculate TF: frequency / total_words
            double tf = totalWords > 0 ? (double) stats.getFrequency() / totalWords : 0.0;

            // create a new document for the posting
            Document posting = new Document().append("doc_id", docId).append("url", url)
                    .append("in_title", inTitle).append("frequency", stats.getFrequency())
                    .append("tf", tf).append("importance_score", stats.getImportanceScore())
                    .append("length", totalWords).append("timestamp", timestamp)
                    .append("positions", stats.getPositions()).append("popularity", popularity)
                    .append("title", title).append("snippet", snippets);

            // for new words, create a new entry in the inverted index
            // for existing words, update the postings array
            // add to bulk operations
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

    // Helper method to select best snippets (up to maxSnippets)
    private List<String> selectBestSnippets(List<String> snippets, int maxSnippets)
    {
        if (snippets.size() <= maxSnippets)
            return snippets;

        snippets.sort((a, b) -> {
            // Primary sort: prefer snippets with complete sentences
            boolean aHasPeriod = a.contains(".");
            boolean bHasPeriod = b.contains(".");
            if (aHasPeriod != bHasPeriod)
                return aHasPeriod ? -1 : 1; // Snippets with periods come first

            // Secondary sort: prefer longer snippets for more context
            return Integer.compare(b.length(), a.length());
        });

        // Return the top maxSnippets
        return snippets.subList(0, maxSnippets);
    }

    // Compute IDF for a word based on the number of documents containing it
    private double computeIdf(int docsWithTerm)
    {
        double idf = Math.log((double) totalDocs / (1 + docsWithTerm));
        return idf;
    }

    // Compute and store IDF values for all words in the inverted index
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
                    // Wait a bit before each check to avoid busy waiting
                    Thread.sleep(500);

                    // Move items from queue to batch
                    while (!bulkOperations.isEmpty() && batch.size() < 1000)
                    {
                        WriteModel<Document> op = bulkOperations.poll();
                        if (op != null)
                            batch.add(op);
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
                        finalBatch.add(op);
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

    // Fetch crawled documents in batches
    public List<CrawledDoc> fetchCrawledDocumentsBatch(int skip, int limit)
    {
        System.out.println(
                "Starting fetchCrawledDocumentsBatch with skip=" + skip + ", limit=" + limit);
        List<CrawledDoc> documents = Collections.synchronizedList(new ArrayList<>());
        List<String> docIdsToClean = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errorCount = new AtomicInteger(0);
        Document query = new Document("$or",
                Arrays.asList(new Document("indexed", false), new Document("indexed", null),
                        new Document("indexed", new Document("$exists", false))));

        // Log query and count matching documents
        long matchingDocs = pagesCollection.countDocuments(query);
        System.out.println("Query: " + query.toJson());
        System.out.println("Found " + matchingDocs + " documents matching query");

        int processorCount = Runtime.getRuntime().availableProcessors();
        System.out.println("Using " + processorCount + " threads for processing");
        ExecutorService executor = Executors.newWorkStealingPool(processorCount);

        // Log documents retrieved by the query
        int docCount = 0;
        for (Document doc : pagesCollection.find(query).skip(skip).limit(limit))
        {
            docCount++;


            executor.submit(() -> {
                try
                {
                    // Validate required fields
                    if (!doc.containsKey("url") || doc.getString("url") == null)
                    {
                        System.err.println("Document missing or null 'url' field: _id="
                                + doc.getObjectId("_id"));
                        errorCount.incrementAndGet();
                        return;
                    }
                    if (!doc.containsKey("content") || doc.getString("content") == null)
                    {
                        System.err.println("Document missing or null 'content' field: _id="
                                + doc.getObjectId("_id"));
                        errorCount.incrementAndGet();
                        return;
                    }
                    if (!doc.containsKey("title") || doc.getString("title") == null)
                    {
                        System.err.println("Document missing or null 'title' field: _id="
                                + doc.getObjectId("_id"));
                        errorCount.incrementAndGet();
                        return;
                    }
                    if (!doc.containsKey("popularity"))
                    {
                        System.err.println("Document missing 'popularity' field: _id="
                                + doc.getObjectId("_id"));
                        errorCount.incrementAndGet();
                        return;
                    }

                    String url = doc.getString("url");
                    String content = doc.getString("content");
                    String title = doc.getString("title");
                    double popularity = doc.getDouble("popularity");

                    // Create CrawledDoc
                    System.out.println("Creating CrawledDoc for URL: " + url);
                    CrawledDoc crawledDoc = new CrawledDoc(url, title, popularity, content);
                    crawledDoc.setMongoId(doc.getObjectId("_id").toString());
                    System.out
                            .println("CrawledDoc created with MongoId: " + doc.getObjectId("_id"));

                    // Check for existing doc_id
                    if (doc.containsKey("doc_id"))
                    {
                        String existingDocId = doc.getString("doc_id");
                        System.out.println("Found existing doc_id: " + existingDocId);
                        Document forwardIndexEntry = forwardIndexCollection
                                .find(Filters.eq("doc_id", existingDocId)).first();
                        if (forwardIndexEntry != null)
                        {
                            System.out.println(
                                    "Forward index entry found for doc_id: " + existingDocId);
                            crawledDoc.setDocId(existingDocId);
                            docIdsToClean.add(existingDocId);
                        }
                        else
                            System.out
                                    .println("No forward index entry for doc_id: " + existingDocId);

                    }
                    else
                        System.out.println(
                                "No doc_id field in document: _id=" + doc.getObjectId("_id"));


                    documents.add(crawledDoc);
                    System.out.println("Added CrawledDoc to documents list for URL: " + url);
                }
                catch (Exception e)
                {
                    errorCount.incrementAndGet();
                    System.err.println("Error processing document _id=" + doc.getObjectId("_id")
                            + ": " + e.getClass().getName() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Retrieved " + docCount + " documents from query");

        // Shutdown executor and wait for completion
        executor.shutdown();
        try
        {
            System.out.println("Waiting for executor tasks to complete...");
            boolean terminated = executor.awaitTermination(5, TimeUnit.MINUTES);
            if (!terminated)
                System.err.println("Executor did not terminate within 5 minutes");
            else
                System.out.println("Executor terminated successfully");

        }
        catch (InterruptedException e)
        {
            System.err.println("Executor interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }

        // Log results
        System.out.println("Batch processing complete: " + documents.size()
                + " documents retrieved, " + errorCount.get() + " errors encountered");
        if (!docIdsToClean.isEmpty())
        {
            System.out.println("Documents to clean: " + docIdsToClean.size() + " doc_ids");
            cleanIndexEntriesForDocuments(docIdsToClean);
        }
        else
            System.out.println("No documents to clean");

        return documents;
    }

    // Clean index entries for documents that are being re-indexed
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

    // main method to start the indexing process
    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        Indexer indexer = new Indexer();

        try
        {
            // Process documents in batches to avoid memory issues
            MongoCollection<Document> pagesCollection =
                    DatabaseConnection.getDatabase().getCollection("pages");
            // Get count
            long totalDocs = pagesCollection.countDocuments();
            int batchSize = 500;

            for (int skip = 0; skip < totalDocs; skip += batchSize)
            {
                // Pass incremental mode flag
                List<CrawledDoc> batch = indexer.fetchCrawledDocumentsBatch(skip, batchSize);
                System.out.println("Retrieved " + batch.size() + " documents to index");

                if (!batch.isEmpty())
                    indexer.indexDocuments(batch);

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
