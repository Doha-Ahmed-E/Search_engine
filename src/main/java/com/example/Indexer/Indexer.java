package com.example.Indexer;

import com.example.DatabaseConnection;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.*;

public class Indexer
{
    private final MongoCollection<Document> pagesCollection;
    private final MongoCollection<Document> indexCollection;

    public Indexer()
    {
        this.pagesCollection = DatabaseConnection.getDatabase().getCollection("pages");
        this.indexCollection = DatabaseConnection.getDatabase().getCollection("index");
    }

    /// Method to start indexing
    /// It retrieves documents from the pages collection, tokenizes the content,
    /// removes stop words, stems the tokens, and stores the index in the index collection.
    /// It also calculates term frequency (TF) and inverse document frequency (IDF) for each term.
    /// The method also extracts word positions from the page and stores them in the index.

    public void startIndexing()
    {
        long totalDocuments = pagesCollection.countDocuments();
        for (Document page : pagesCollection.find())
        {
            String url = page.getString("url");
            String content = page.getString("content");

            List<String> tokens = Tokenizer.tokenize(content);
            tokens = StopWords.removeStopWords(tokens);
            tokens.replaceAll(Stemmer::stem);

            Map<String, List<String>> wordPositions = extractWordPositions(page);

            storeIndex(url, tokens, wordPositions, totalDocuments);
        }
    }

    // Method to store the index in the index collection
    // It calculates term frequency (TF) and inverse document frequency (IDF) for each term
    // and stores the term, document count, and positions in the index collection.
    // The method also updates the index if the term already exists, using $setOnInsert and $push.
    // The positions are sorted based on their priority (title, heading, body).

    private void storeIndex(String url, List<String> tokens,
            Map<String, List<String>> wordPositions, long totalDocuments)
    {
        int totalWords = tokens.size();
        Map<String, Integer> termFrequency = new HashMap<>();

        for (String term : tokens)
        {
            termFrequency.put(term, termFrequency.getOrDefault(term, 0) + 1);
        }

        for (String term : termFrequency.keySet())
        {
            double tf = (double) termFrequency.get(term) / totalWords;

            // Store IDF and document count only once
            long docCount = indexCollection.countDocuments(new Document("term", term));
            double idf = Math.log((double) totalDocuments / (docCount + 1));

            List<String> positions = wordPositions.getOrDefault(term, List.of("body"));
            positions.sort(Comparator.comparingInt(pos -> getPriority(pos)));

            Document docEntry = new Document("url", url).append("tf", tf)
                    .append("positions", positions).append("count", termFrequency.get(term));

            indexCollection.updateOne(new Document("term", term),
                    new Document("$setOnInsert",
                            new Document("idf", idf).append("doc_count", totalDocuments))
                                    .append("$push", new Document("documents", docEntry)),
                    new com.mongodb.client.model.UpdateOptions().upsert(true));
        }

        System.out.println("Indexed: " + url);
    }

    // Method to extract word positions from the page
    // It retrieves the title, headings, and body content from the page
    // and tokenizes them to create a mapping of words to their positions.
    // The positions are stored in a map where the key is the word and the value is a list of
    // positions.
    // The method uses the Tokenizer class to tokenize the content and the StopWords class to remove
    // stop words.

    private Map<String, List<String>> extractWordPositions(Document page)
    {
        Map<String, List<String>> wordPositions = new HashMap<>();

        // Assume the page has "title", "headings", and "body" fields
        String title = page.getString("title");
        List<String> headings = page.getList("headings", String.class);
        String body = page.getString("content");

        addWordsToPositions(wordPositions, Tokenizer.tokenize(title), "title");
        for (String heading : headings)
        {
            addWordsToPositions(wordPositions, Tokenizer.tokenize(heading), "heading");
        }
        addWordsToPositions(wordPositions, Tokenizer.tokenize(body), "body");

        return wordPositions;
    }

    // Method to add words to the word positions map
    // It takes a map of word positions, a list of words, and a position string
    // and adds the words to the map with their corresponding position.
    // The method uses the computeIfAbsent method to create a new list if the word is not already
    // present in the map.
    // The position is added to the list of positions for each word.
    // The method is used to build the word positions map for the page.

    private void addWordsToPositions(Map<String, List<String>> wordPositions, List<String> words,
            String position)
    {
        for (String word : words)
        {
            wordPositions.computeIfAbsent(word, k -> new ArrayList<>()).add(position);
        }
    }

    // Method to get the priority of a position
    // It takes a position string (title, heading, body) and returns an integer priority value.

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

    // Main method to start the indexing process
    // It creates an instance of the Indexer class and calls the startIndexing method.

    public static void main(String[] args)
    {
        new Indexer().startIndexing();
    }
}
