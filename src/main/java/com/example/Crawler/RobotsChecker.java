package com.example.Crawler;

import org.jsoup.Jsoup;
import java.io.IOException;

public class RobotsChecker
{
    public static boolean isAllowed(String url)
    {
        try
        {
            String robotsUrl = url + "/robots.txt";
            String robotsTxt = Jsoup.connect(robotsUrl).ignoreContentType(true).execute().body();

            return !robotsTxt.contains("Disallow: /"); // If Disallowed, return false
        }
        catch (IOException e)
        {
            System.out.println("No robots.txt found, assuming allowed.");
            return true;
        }
    }
}
