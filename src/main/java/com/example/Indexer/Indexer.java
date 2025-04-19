package com.example.Indexer;

import com.example.DatabaseConnection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Indexer
{
    private final MongoCollection<Document> pagesCollection;
    private final MongoCollection<Document> indexCollection;
    private final int numThreads;
    private final ExecutorService executorService;
    private final AtomicInteger processedCount = new AtomicInteger(0);

    // Debugging flag
    private static final boolean DEBUG = true;

    public Indexer(int numThreads)
    {
        System.out.println("Initializing indexer with " + numThreads + " threads");
        this.pagesCollection = DatabaseConnection.getDatabase().getCollection("pages");
        this.indexCollection = DatabaseConnection.getDatabase().getCollection("index");
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);

        // Test database connection
        try
        {
            long count = pagesCollection.countDocuments();
            System.out.println("Successfully connected to database. Pages collection has " + count
                    + " documents.");
        }
        catch (Exception e)
        {
            System.err.println("❌ ERROR: Failed to connect to database: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Database connection failed", e);
        }
    }

    public void startIndexing()
    {
        System.out.println("Starting indexing process...");

        try
        {
            // Get cursor instead of loading all documents at once
            MongoCursor<Document> cursor = pagesCollection.find().iterator();

            // Create and submit tasks
            System.out.println("Creating worker tasks...");
            List<Future<?>> futures = new ArrayList<>();
            BlockingQueue<Document> documentQueue = new LinkedBlockingQueue<>(1000);

            // Start producer thread to fill the queue
            Future<?> producerFuture = executorService.submit(() -> {
                try
                {
                    int count = 0;
                    while (cursor.hasNext())
                    {
                        documentQueue.put(cursor.next());
                        count++;
                        if (count % 100 == 0)
                        {
                            System.out.println("Producer: Added " + count + " documents to queue");
                        }
                    }
                    // Add sentinel values to signal the end of documents
                    for (int i = 0; i < numThreads; i++)
                    {
                        documentQueue.put(new Document("_SENTINEL_", true));
                    }
                    System.out.println("Producer: Finished adding all documents to queue");
                }
                catch (Exception e)
                {
                    System.err.println("❌ Producer error: " + e.getMessage());
                    e.printStackTrace();
                } finally
                {
                    cursor.close();
                }
            });
            futures.add(producerFuture);

            // Start worker threads to process documents from the queue
            for (int i = 0; i < numThreads; i++)
            {
                final int workerId = i;
                Future<?> future = executorService.submit(() -> {
                    String threadName = "Worker-" + workerId;
                    System.out.println(threadName + " started");

                    try
                    {
                        List<Document> indexBatch = new ArrayList<>();
                        int localProcessed = 0;

                        while (true)
                        {
                            Document page = documentQueue.take();

                            // Check for sentinel
                            if (page.containsKey("_SENTINEL_"))
                            {
                                break;
                            }

                            try
                            {
                                processDocument(page, indexBatch);
                                localProcessed++;

                                // Commit batch periodically
                                if (indexBatch.size() >= 50)
                                {
                                    if (DEBUG)
                                        System.out.println(threadName + ": Committing batch of "
                                                + indexBatch.size() + " entries");
                                    commitIndexBatch(indexBatch, threadName);
                                    indexBatch.clear();
                                    System.out.println(threadName + ": Processed " + localProcessed
                                            + " documents");
                                }
                            }
                            catch (Exception e)
                            {
                                System.err.println(threadName + " ❌ Error processing document: "
                                        + e.getMessage());
                                e.printStackTrace();
                            }
                        }

                        // Process remaining batch
                        if (!indexBatch.isEmpty())
                        {
                            if (DEBUG)
                                System.out.println(threadName + ": Committing final batch of "
                                        + indexBatch.size() + " entries");
                            commitIndexBatch(indexBatch, threadName);
                        }

                        System.out.println(threadName + " finished. Processed " + localProcessed
                                + " documents");
                        processedCount.addAndGet(localProcessed);

                    }
                    catch (InterruptedException e)
                    {
                        System.err.println(threadName + " was interrupted");
                        Thread.currentThread().interrupt();
                    }
                    catch (Exception e)
                    {
                        System.err.println(threadName + " encountered an error: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
                futures.add(future);
            }

            // Wait for all tasks to complete with timeout
            for (Future<?> future : futures)
            {
                try
                {
                    // Set a timeout to prevent indefinite hanging
                    future.get(10, TimeUnit.MINUTES);
                }
                catch (TimeoutException e)
                {
                    System.err.println(
                            "❌ A task timed out after 10 minutes. Cancelling remaining tasks.");
                    future.cancel(true);
                }
                catch (InterruptedException | ExecutionException e)
                {
                    System.err.println("❌ Error in indexing thread: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println(
                    "✅ Indexing complete. Total processed documents: " + processedCount.get());

        }
        catch (Exception e)
        {
            System.err.println("❌ Fatal error in indexing process: " + e.getMessage());
            e.printStackTrace();
        } finally
        {
            // Ensure shutdown
            shutdownExecutor();
        }
    }

    private void processDocument(Document page, List<Document> indexBatch)
    {
        String url = Optional.ofNullable(page.getString("url")).orElse("");
        String content = Optional.ofNullable(page.getString("content")).orElse("");

        if (url.isBlank() || content.isBlank())
        {
            if (DEBUG)
                System.err.println("⚠️ Skipping document with missing URL or content.");
            return;
        }

        // Process content
        List<String> tokens = new ArrayList<>(Tokenizer.tokenize(content));
        if (DEBUG && tokens.isEmpty())
        {
            System.err.println("⚠️ No tokens extracted from content for URL: " + url);
        }

        tokens = new ArrayList<>(StopWords.removeStopWords(tokens));
        tokens.removeIf(word -> word == null || word.isBlank());

        // Apply stemming
        for (int i = 0; i < tokens.size(); i++)
        {
            tokens.set(i, Stemmer.stem(tokens.get(i)));
        }

        // Process positions
        Map<String, List<String>> wordPositions = extractWordPositions(page);

        // Create index entries
        int totalWords = tokens.size();
        Map<String, Integer> termFrequency = new HashMap<>();

        // Count term frequencies
        for (String term : tokens)
        {
            termFrequency.put(term, termFrequency.getOrDefault(term, 0) + 1);
        }

        // Create documents for batch
        for (String term : termFrequency.keySet())
        {
            double tf = (double) termFrequency.get(term) / Math.max(1, totalWords);

            // Create positions list
            List<String> positions = new ArrayList<>();
            List<String> termPositions = wordPositions.get(term);
            if (termPositions != null)
            {
                positions.addAll(termPositions);
            }
            else
            {
                positions.add("body");
            }
            positions.sort(Comparator.comparingInt(this::getPriority));

            // Create document for batch
            Document entry = new Document().append("term", term).append("url", url).append("tf", tf)
                    .append("positions", positions).append("count", termFrequency.get(term));

            indexBatch.add(entry);
        }
    }

    private void commitIndexBatch(List<Document> batch, String threadName)
    {
        if (batch.isEmpty())
            return;

        try
        {
            // Group by term
            Map<String, List<Document>> termGroups = new HashMap<>();
            for (Document doc : batch)
            {
                String term = doc.getString("term");
                if (term == null)
                {
                    System.err.println("⚠️ Document missing term field: " + doc.toJson());
                    continue;
                }
                termGroups.computeIfAbsent(term, k -> new ArrayList<>()).add(doc);
            }

            // Process each term
            int termsProcessed = 0;
            for (Map.Entry<String, List<Document>> entry : termGroups.entrySet())
            {
                String term = entry.getKey();
                List<Document> docs = entry.getValue();

                // Skip empty lists
                if (docs.isEmpty())
                    continue;

                try
                {
                    // Calculate IDF
                    long docCount = indexCollection.countDocuments(new Document("term", term));
                    long totalDocuments = pagesCollection.countDocuments();
                    double idf = Math.log((double) totalDocuments / (docCount + 1));

                    // Prepare document entries
                    List<Document> documentEntries = new ArrayList<>(docs.size());
                    for (Document doc : docs)
                    {
                        // Create a new document without the term field
                        Document ent = new Document();
                        for (String key : doc.keySet())
                        {
                            if (!key.equals("term"))
                            {
                                ent.append(key, doc.get(key));
                            }
                        }
                        documentEntries.add(ent);
                    }

                    // Update the index
                    indexCollection.updateOne(new Document("term", term),
                            new Document("$set",
                                    new Document("idf", idf).append("doc_count", totalDocuments))
                                            .append("$push",
                                                    new Document("documents",
                                                            new Document("$each",
                                                                    documentEntries))),
                            new com.mongodb.client.model.UpdateOptions().upsert(true));

                    termsProcessed++;
                    if (DEBUG && termsProcessed % 100 == 0)
                    {
                        System.out.println(threadName + ": Processed " + termsProcessed + " terms");
                    }
                }
                catch (Exception e)
                {
                    System.err.println("❌ Error updating term '" + term + "': " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println(
                    threadName + ": Successfully committed " + termsProcessed + " terms to index");

        }
        catch (Exception e)
        {
            System.err.println(threadName + " ❌ Error committing batch: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Map<String, List<String>> extractWordPositions(Document page)
    {
        Map<String, List<String>> wordPositions = new HashMap<>();

        String title = Optional.ofNullable(page.getString("title")).orElse("");
        List<String> headings = Optional.ofNullable(page.getList("headings", String.class))
                .orElse(new ArrayList<>());
        String body = Optional.ofNullable(page.getString("content")).orElse("");

        addWordsToPositions(wordPositions, Tokenizer.tokenize(title), "title");
        for (String heading : headings)
        {
            addWordsToPositions(wordPositions, Tokenizer.tokenize(heading), "heading");
        }
        addWordsToPositions(wordPositions, Tokenizer.tokenize(body), "body");

        return wordPositions;
    }

    private void addWordsToPositions(Map<String, List<String>> wordPositions, List<String> words,
            String position)
    {
        for (String word : words)
        {
            String stemmedWord = Stemmer.stem(word);
            wordPositions.computeIfAbsent(stemmedWord, k -> new ArrayList<>()).add(position);
        }
    }

    private int getPriority(String pos)
    {
        return switch (pos)
        {
            case "title" -> 1;
            case "heading" -> 2;
            case "body" -> 3;
            default -> 4;
        };
    }

    private void shutdownExecutor()
    {
        System.out.println("Shutting down executor service...");
        executorService.shutdown();
        try
        {
            // Wait for pending tasks to finish
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS))
            {
                System.out.println("Forcing shutdown after timeout...");
                executorService.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args)
    {
        System.out.println("Starting multi-threaded indexing process with detailed logging...");
        try
        {
            // Create indexer with 5 threads
            int numThreads = 5;
            new Indexer(numThreads).startIndexing();
        }
        catch (Exception e)
        {
            System.err.println("❌ Fatal error during indexing: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
