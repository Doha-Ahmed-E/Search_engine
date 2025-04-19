# searchEngine

crawler ===> structure done ☑️
                
        Key Features Added:
                        Complete URL Normalization - Proper handling of different URL formats

                        Robots.txt Compliance - Full parsing and caching of robots.txt rules

                        Rate Limiting - Per-domain request rate limiting

                        Content-Type Checking - Verifies only HTML content is processed

                        Improved Thread Safety - Better synchronization in all components

                        Graceful Shutdown - Proper shutdown handling with Runtime hook

                        Enhanced Link Scoring - More sophisticated prioritization algorithm

                        Bloom Filter Option - Ready for scaling with Bloom filters

                        State Persistence - Structure in place for saving/loading crawl state

                        Error Handling - Comprehensive exception handling throughout

                        
        ===> scheduling and updating content                

Indexer ===> very basic layout ☑️
        ===> fully integrate the database, think about what to store ☑️
        
        Update Indexer.java
        Update Indexer.java to use the standalone CrawledDoc class instead of the nested CrawledDocument class. Replace all references to CrawledDocument with CrawledDoc.
        Removed the nested CrawledDocument class.
        Updated all references to CrawledDocument to CrawledDoc.
        Updated the processText method to use the new removeStopWords method from StopWords.java, which now returns a mutable List<String>.
        

        Your .env file specifies DB_NAME=web_crawler, but DatabaseConnection.java is hardcoded to use the database SearchDB. This mismatch means the Indexer is looking in the wrong database. Let’s update DatabaseConnection.java to use the database name from the .env file.

        ------------------------------------------------------------------------------------------

       update tokenizer to make sure numbers and special characters (#, !,. ...etc) are not processed
       update doc count inside the loop (instead of iterating the database agian after indexing)
       
