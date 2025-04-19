package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class DatabaseConnection {
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    static {
        Dotenv dotenv = Dotenv.load();
        String uri = dotenv.get("MONGO_URI");
        String dbName = dotenv.get("DB_NAME"); // Read DB_NAME from .env

        if (uri == null || uri.isEmpty()) {
            throw new IllegalStateException("MONGO_URI environment variable is not set");
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalStateException("DB_NAME environment variable is not set");
        }

        try {
            mongoClient = MongoClients.create(uri);
            database = mongoClient.getDatabase(dbName); // Use dbName from .env
            System.out.println("Connected to MongoDB database: " + dbName);
        } catch (Exception e) {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    public static MongoDatabase getDatabase() {
        return database;
    }

    public static void closeConnection() {
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("MongoDB connection closed.");
        }
    }
}