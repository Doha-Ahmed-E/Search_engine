package com.example.util;

import org.bson.Document;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for safe type casting operations Centralizes handling of unchecked casts and
 * improves code readability
 */
public class TypeSafeUtil
{

    /**
     * Safely cast an Object to List<String>
     * 
     * @param obj The object to cast
     * @return A List<String> or empty list if conversion isn't possible
     */
    public static List<String> safeStringList(Object obj)
    {
        if (obj == null)
        {
            return Collections.emptyList();
        }

        if (obj instanceof List<?>)
        {
            List<?> list = (List<?>) obj;
            List<String> result = new ArrayList<>();

            for (Object item : list)
            {
                if (item instanceof String)
                {
                    result.add((String) item);
                }
            }
            return result;
        }

        return Collections.emptyList();
    }

    /**
     * Safely cast an Object to List<Document>
     * 
     * @param obj The object to cast
     * @return A List<Document> or empty list if conversion isn't possible
     */
    public static List<Document> safeDocumentList(Object obj)
    {
        if (obj == null)
        {
            return Collections.emptyList();
        }

        if (obj instanceof List<?>)
        {
            List<?> list = (List<?>) obj;
            List<Document> result = new ArrayList<>();

            for (Object item : list)
            {
                if (item instanceof Document)
                {
                    result.add((Document) item);
                }
            }
            return result;
        }

        return Collections.emptyList();
    }

    /**
     * Safely extract a string value from a document
     * 
     * @param doc The document
     * @param key The key to extract
     * @return The string value or empty string if not found/not a string
     */
    public static String safeString(Document doc, String key)
    {
        if (doc == null || key == null)
        {
            return "";
        }

        Object value = doc.get(key);
        return value instanceof String ? (String) value : "";
    }

    /**
     * Safely extract an integer value from a document
     * 
     * @param doc The document
     * @param key The key to extract
     * @param defaultValue Value to return if key doesn't exist or isn't an Integer
     * @return The integer value or defaultValue if not found/not an integer
     */
    public static int safeInteger(Document doc, String key, int defaultValue)
    {
        if (doc == null || key == null)
        {
            return defaultValue;
        }

        Object value = doc.get(key);
        return value instanceof Integer ? (Integer) value : defaultValue;
    }

    /**
     * Safely extract a double value from a document
     * 
     * @param doc The document
     * @param key The key to extract
     * @param defaultValue Value to return if key doesn't exist or isn't a Number
     * @return The double value or defaultValue if not found/not a number
     */
    public static double safeDouble(Document doc, String key, double defaultValue)
    {
        if (doc == null || key == null)
        {
            return defaultValue;
        }

        Object value = doc.get(key);
        if (value instanceof Number)
        {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }

    /**
     * Safely extract a boolean value from a document
     * 
     * @param doc The document
     * @param key The key to extract
     * @param defaultValue Value to return if key doesn't exist or isn't a Boolean
     * @return The boolean value or defaultValue if not found/not a boolean
     */
    public static boolean safeBoolean(Document doc, String key, boolean defaultValue)
    {
        if (doc == null || key == null)
        {
            return defaultValue;
        }

        Object value = doc.get(key);
        return value instanceof Boolean ? (Boolean) value : defaultValue;
    }
}
