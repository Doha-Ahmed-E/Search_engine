package com.example.Crawler;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;

//Purpose: Generates unique hashes for web page content to detect duplicates.
//Uses SHA-256 hashing algorithm for strong collision resistance
//Converts the content byte array to a hexadecimal string representation
//Wraps exceptions in RuntimeException for easier error handling

public class PageHasher {
    private static final String HASH_ALGORITHM = "SHA-256";
    
    public static String generateHash(String content) {
        try {
            MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hashBytes = md.digest(content.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, hashBytes).toString(16);
        } catch (Exception e) {
            throw new RuntimeException("Hashing error: " + e.getMessage(), e);
        }
    }
}