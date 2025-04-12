
package com.example.Crawler;
import java.net.URI;

import java.net.URL;

//Purpose: Standardizes URLs to prevent duplicate crawling.
//Converts protocol and host to lowercase
//Removes default ports (e.g., :80 for HTTP)
//Normalizes paths (resolves ./ and ../)
//Preserves query strings but removes fragments (#)

public class UrlNormalizer {
    public static String normalize(String url) {
        try {
            URL u = new URL(url);
            
            // Normalize protocol and host
            String protocol = u.getProtocol().toLowerCase();
            String host = u.getHost().toLowerCase();
            
            // Handle default ports
            int port = u.getPort();
            if (port == u.getDefaultPort()) {
                port = -1;
            }
            
            // Normalize path
            String path = u.getPath();
            if (path == null || path.isEmpty()) {
                path = "/";
            }
            
            // Remove fragments and normalize path
            URI uri = new URI(u.getProtocol(), u.getUserInfo(), u.getHost(), 
                            port, u.getPath(), u.getQuery(), null);
            path = uri.normalize().getPath();
            
            // Rebuild URL
            StringBuilder result = new StringBuilder();
            result.append(protocol).append("://").append(host);
            if (port != -1) {
                result.append(":").append(port);
            }
            result.append(path);
            if (u.getQuery() != null) {
                result.append("?").append(u.getQuery());
            }
            
            return result.toString();
        } catch (Exception e) {
            System.err.println("URL normalization failed for: " + url);
            return null;
        }
    }
}