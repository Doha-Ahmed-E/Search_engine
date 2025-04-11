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

    public void startIndexing()
    {
        for (Document page : pagesCollection.find())
        {
            String url = page.getString("url");
            String content = page.getString("content");

            List<String> tokens = Tokenizer.tokenize(content);
            tokens = StopWords.removeStopWords(tokens);
            tokens.replaceAll(Stemmer::stem);

            storeIndex(url, tokens);
        }
    }

    private void storeIndex(String url, List<String> tokens)
    {
        for (String term : tokens)
        {
            indexCollection.updateOne(new Document("term", term),
                    new Document("$push", new Document("urls", url)),
                    new com.mongodb.client.model.UpdateOptions().upsert(true));
        }
        System.out.println("✅ Indexed: " + url);
    }

    public static void main(String[] args)
    {
        new Indexer().startIndexing();
    }
}
