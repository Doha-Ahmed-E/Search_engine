package com.example.Indexer;

import com.example.DatabaseConnection; // From com.example
import com.example.Crawler.PageHasher; // From com.example.Crawler
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Indexer
{
    private final MongoCollection<Document> wordIndexCollection;
    private final MongoCollection<Document> documentsCollection;
    private final AtomicInteger docIdCounter = new AtomicInteger(0);
    private final Tokenizer tokenizer;
    private final StopWords stopWords;
    private final Stemmer stemmer;

    private static final double TITLE_WEIGHT = 3.0;
    private static final double HEADER_WEIGHT = 2.0;
    private static final double BODY_WEIGHT = 1.0;

    public Indexer()
    {
        this.wordIndexCollection = DatabaseConnection.getDatabase().getCollection("word_index");
        this.documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
        this.tokenizer = new Tokenizer();
        this.stopWords = new StopWords();
        this.stemmer = new Stemmer();

        // Create indexes for fast retrieval
        wordIndexCollection.createIndex(new Document("word", 1));
        documentsCollection.createIndex(new Document("doc_id", 1));
        documentsCollection.createIndex(new Document("url", 1));
    }

    public void indexDocuments(List<CrawledDoc> crawledDocs)
    {
        for (CrawledDoc crawledDoc : crawledDocs)
        {
            try
            {
                indexSingleDocument(crawledDoc);
            }
            catch (Exception e)
            {
                System.err.println("Failed to index document: " + crawledDoc.getUrl() + " - "
                        + e.getMessage());
            }
        }
        System.out.println("Indexing completed. Total documents indexed: " + docIdCounter.get());
    }

    private void indexSingleDocument(CrawledDoc crawledDoc)
    {
        String url = crawledDoc.getUrl();
        String htmlContent = crawledDoc.getHtmlContent();
        String contentHash = PageHasher.generateHash(htmlContent);

        // Check for duplicates
        Document existingDoc =
                documentsCollection.find(Filters.eq("content_hash", contentHash)).first();
        if (existingDoc != null)
        {
            System.out.println("Document already indexed, skipping: " + url);
            return;
        }

        // Parse HTML content
        org.jsoup.nodes.Document doc = Jsoup.parse(htmlContent, url);
        String title = doc.title();
        String docId = "doc_" + docIdCounter.incrementAndGet();

        // Extract words and compute statistics
        Map<String, WordStats> wordStats = new HashMap<>();
        int totalWords = 0;

        // Process title
        totalWords += processText(title, TITLE_WEIGHT, wordStats);

        // Process headers (h1–h6)
        for (int i = 1; i <= 6; i++)
        {
            Elements headers = doc.select("h" + i);
            for (Element header : headers)
            {
                totalWords += processText(header.text(), HEADER_WEIGHT, wordStats);
            }
        }

        // Process body text
        totalWords += processText(doc.body().text(), BODY_WEIGHT, wordStats);

        // Save document metadata
        Document docEntry = new Document().append("doc_id", docId).append("url", url)
                .append("title", title).append("content_hash", contentHash)
                .append("total_words", totalWords).append("timestamp", System.currentTimeMillis());
        documentsCollection.insertOne(docEntry);

        // Update word index with TF
        updateWordIndex(docId, url, wordStats, totalWords);
        System.out.println("Indexed document: " + url + " (ID: " + docId + ")");
    }

    private int processText(String text, double weight, Map<String, WordStats> wordStats)
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
            // Stem the token
            String stemmedWord = stemmer.stem(token);
            if (stemmedWord == null || stemmedWord.isEmpty())
                continue;

            // Update word statistics
            wordStats.computeIfAbsent(stemmedWord, k -> new WordStats()).addOccurrence(weight);
            wordCount++;
        }
        return wordCount;
    }

    private void updateWordIndex(String docId, String url, Map<String, WordStats> wordStats,
            int totalWords)
    {
        for (Map.Entry<String, WordStats> entry : wordStats.entrySet())
        {
            String word = entry.getKey();
            WordStats stats = entry.getValue();

            // Calculate TF: frequency / total_words
            double tf = totalWords > 0 ? (double) stats.getFrequency() / totalWords : 0.0;

            Document posting = new Document().append("doc_id", docId).append("url", url)
                    .append("frequency", stats.getFrequency()).append("tf", tf)
                    .append("importance_score", stats.getImportanceScore());

            wordIndexCollection.updateOne(Filters.eq("word", word),
                    new Document("$push", new Document("postings", posting)).append("$inc",
                            new Document("doc_count", 1)), // ✅ Increment doc_count
                    new UpdateOptions().upsert(true));

        }
    }

    public List<CrawledDoc> fetchCrawledDocuments()
    {
        List<CrawledDoc> documents = new ArrayList<>();
        MongoCollection<Document> pagesCollection =
                DatabaseConnection.getDatabase().getCollection("pages");

        for (Document doc : pagesCollection.find())
        {
            String url = doc.getString("url");
            String content = doc.getString("content");
            documents.add(new CrawledDoc(url, content));
        }

        return documents;
    }

    public static void main(String[] args)
    {
        Indexer indexer = new Indexer();
        List<CrawledDoc> crawledDocs = indexer.fetchCrawledDocuments();
        indexer.indexDocuments(crawledDocs);
        DatabaseConnection.closeConnection(); // Close connection when done
    }
}
