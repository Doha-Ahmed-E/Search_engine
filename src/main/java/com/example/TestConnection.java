package com.example;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class TestConnection
{
    public static void main(String[] args)
    {
        System.out.println("Testing MongoDB connection...");

        // Retrieve database connection
        MongoCollection<Document> collection =
                DatabaseConnection.getDatabase().getCollection("pages");

        System.out.println("✅ Connection to MongoDB is active! Found collection: "
                + collection.getNamespace());

        try
        {
            Thread.sleep(120000);
        }
        catch (InterruptedException e)
        {
            System.err.println("Sleep interrupted: " + e.getMessage());
        }

        DatabaseConnection.closeConnection();
    }
}
