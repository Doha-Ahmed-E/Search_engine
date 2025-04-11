package com.example.Crawler;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;

public class PageHasher
{
    public static String generateHash(String content)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = md.digest(content.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, hashBytes).toString(16);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Hashing error: " + e.getMessage());
        }
    }
}
