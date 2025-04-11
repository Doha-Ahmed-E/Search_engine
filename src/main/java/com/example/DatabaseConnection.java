package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class DatabaseConnection
{
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    static
    {
        Dotenv dotenv = Dotenv.load();
        String uri = dotenv.get("MONGO_URI");

        try
        {
            mongoClient = MongoClients.create(uri);
            database = mongoClient.getDatabase("SearchDB");
            System.out.println("Connected to MongoDB!");
        }
        catch (Exception e)
        {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    // Provide access to the database across different files
    public static MongoDatabase getDatabase()
    {
        return database;
    }

    // Only close when you want to shut down the application completely
    public static void closeConnection()
    {
        if (mongoClient != null)
        {
            mongoClient.close();
            System.out.println("MongoDB connection closed.");
        }
    }

}
