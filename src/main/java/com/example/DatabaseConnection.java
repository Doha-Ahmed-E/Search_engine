package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;

import java.util.concurrent.TimeUnit;

public class DatabaseConnection
{
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    static
    {
        Dotenv dotenv = Dotenv.load();
        String uri = dotenv.get("MONGO_URI");
        String dbName = dotenv.get("DB_NAME"); // Read DB_NAME from .env

        if (uri == null || uri.isEmpty())
            throw new IllegalStateException("MONGO_URI environment variable is not set");
        if (dbName == null || dbName.isEmpty())
            throw new IllegalStateException("DB_NAME environment variable is not set");
        try
        {
            MongoClientSettings settings =
                    MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                            .applyToConnectionPoolSettings(builder -> builder.maxSize(100)
                                    .minSize(20).maxWaitTime(30000, TimeUnit.MILLISECONDS))
                            .applyToSocketSettings(
                                    builder -> builder.connectTimeout(60000, TimeUnit.MILLISECONDS)
                                            .readTimeout(60000, TimeUnit.MILLISECONDS))
                            .retryWrites(true).retryReads(true).build();

            mongoClient = MongoClients.create(settings);
            database = mongoClient.getDatabase(dbName); // Use dbName from .env
            System.out.println("Connected to MongoDB database: " + dbName);
        }
        catch (Exception e)
        {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    public static MongoDatabase getDatabase()
    {
        return database;
    }

    public static MongoClient getMongoClient()
    {
        return mongoClient;
    }
    
    public static void closeConnection()
    {
        if (mongoClient != null)
        {
            mongoClient.close();
            System.out.println("MongoDB connection closed.");
        }
    }
}
