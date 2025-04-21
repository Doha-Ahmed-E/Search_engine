package com.example.QueryProcessor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.example.DatabaseConnection;
import com.example.Indexer.Stemmer;
import com.example.Indexer.StopWords;
import com.example.Indexer.Tokenizer;
import com.example.Ranker.QueryInput;
import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

public class QueryProcessor
{
	private final MongoCollection<Document> wordIndexCollection;
	private final MongoCollection<Document> documentsCollection;
	private final Tokenizer tokenizer;
	private final StopWords stopWords;
	private final Stemmer stemmer;
	private final ObjectMapper objectMapper;

	public QueryProcessor()
	{
		wordIndexCollection = DatabaseConnection.getDatabase().getCollection("word_index");
		documentsCollection = DatabaseConnection.getDatabase().getCollection("documents");
		tokenizer = new Tokenizer();
		stopWords = new StopWords();
		stemmer = new Stemmer();
		objectMapper = new ObjectMapper();
	}

	public QueryInput processQuery(String queryString, int maxResults)
	{
		try
		{
			// Preprocess query
			List<String> queryTerms = preprocessQuery(queryString);
			if (queryTerms.isEmpty())
				return new QueryInput();


			// Prepare the QueryInput object
			QueryInput queryInput = new QueryInput();
			queryInput.setQueryTerms(queryTerms);


			Map<String, Integer> docsContainingTerm = new HashMap<>();
			GlobalStats globalStats = new GlobalStats();
			Map<String, QDocument> candidateDocs = new HashMap<>();
			candidateDocs = collectDocumentInfo(queryTerms, docsContainingTerm, globalStats);

			queryInput.setCandidateDocuments(candidateDocs);
			queryInput.setGlobalStats(globalStats);

			return queryInput;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return new QueryInput();
		}
	}

	// Preprocess the query string
	private List<String> preprocessQuery(String queryString)
	{
		List<String> tokens = tokenizer.tokenize(queryString);
		tokens = stopWords.removeStopWords(tokens);

		List<String> stemmedTokens = new ArrayList<>();
		for (String token : tokens)
		{
			String stemmed = stemmer.stem(token);
			if (stemmed != null && !stemmed.isEmpty())
			{
				stemmedTokens.add(stemmed);
			}
		}
		return stemmedTokens;
	}


	// Collect document information and term statistics
	private Map<String, QDocument> collectDocumentInfo(List<String> queryTerms,
			Map<String, Integer> docsContainingTerm, GlobalStats globalStats)
	{

		Map<String, QDocument> candidateDocs = new HashMap<>();
		Set<String> docsWithMetadata = new HashSet<>();

		// Process each query term
		for (String term : queryTerms)
		{
			// Find term in word_index
			Document wordDoc = wordIndexCollection.find(Filters.eq("word", term)).first();
			if (wordDoc == null)
				continue;

			// Get doc-count for the term
			Integer docFrequency = wordDoc.getInteger("doc_count");
			docsContainingTerm.put(term, docFrequency);

			// Process each document containing this term
			List<Document> postings = wordDoc.getList("postings", Document.class);
			if (postings == null)
				continue;
			for (Document posting : postings)
			{
				String docId = posting.getString("doc_id");
				QDocument qDoc = processTermStatistics(candidateDocs, term, docId, posting);
				if (!docsWithMetadata.contains(docId))
				{
					setDocumentMetadata(qDoc, posting);
					docsWithMetadata.add(docId);
				}
			}
		}
		// Set global stats
		long totalDocsCount = documentsCollection.countDocuments();
		globalStats.setTotalDocs((int) totalDocsCount);
		globalStats.setDocsContainingTerm(docsContainingTerm);

		return candidateDocs;
	}

	// Process term statistics for a document
	private QDocument processTermStatistics(Map<String, QDocument> candidateDocs, String term,
			String docId, Document posting)
	{

		double tf = posting.getDouble("tf");
		double importanceScore = posting.getDouble("importance_score");

		// Get or create QDocument in our map
		QDocument qDoc = candidateDocs.computeIfAbsent(docId, k -> new QDocument());

		// Get or create the termStats map for this document
		Map<String, TermStats> termInfoMap = qDoc.getTermStats();
		if (termInfoMap == null)
		{
			termInfoMap = new HashMap<>();
			qDoc.setTermStats(termInfoMap);
		}
		// Add term statistics
		TermStats termStats = new TermStats(false);
		termStats.setTf(tf);
		termStats.setImportanceScore(importanceScore);
		termInfoMap.put(term, termStats);

		return qDoc;
	}

	// Set metadata for a document
	private void setDocumentMetadata(QDocument qDoc, Document posting)
	{
		Metadata metadata = new Metadata();
		metadata.setUrl(posting.getString("url"));
		metadata.setPopularity(0.5);
		metadata.setLength(posting.getInteger("total_words", 0));

		Long timestamp = posting.getLong("timestamp");
		if (timestamp != null)
		{
			Date date = new Date(timestamp);
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			metadata.setPublishDate(formatter.format(date));
		}
		else
			metadata.setPublishDate("");

		qDoc.setMetadata(metadata);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////
	// For testing/debugging
	public void saveQueryResponseToFile(String query, String filename)
	{
		try
		{
			QueryInput queryInput = processQuery(query, 10);
			objectMapper.writeValue(new File(filename), queryInput);
			System.out.println("Query response saved to " + filename);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Main method for direct testing
	public static void main(String[] args)
	{
		try
		{
			// Initialize the QueryProcessor
			QueryProcessor processor = new QueryProcessor();

			// Default query if none provided
			String query = "stack and Java # programming maybe sql";
			if (args.length > 0)
			{
				query = args[0];
			}

			System.out.println("\nProcessing query: \"" + query + "\"");
			long startTime = System.currentTimeMillis();

			// Process the query
			QueryInput result = processor.processQuery(query, 10);

			long endTime = System.currentTimeMillis();
			System.out.println("Query processing time: " + (endTime - startTime) + "ms\n");

			// Print the result details
			printQueryResults(result);

			// Save to file
			String filename = "query_result_" + System.currentTimeMillis() + ".json";
			processor.saveQueryResponseToFile(query, filename);
			System.out.println("\nDetailed results saved to: " + filename);

		}
		catch (Exception e)
		{
			System.err.println("Error processing query: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// Helper method to print results in a readable format
	private static void printQueryResults(QueryInput result)
	{
		// Print query terms
		System.out.println("Query terms: " + result.getQueryTerms());

		// Print global stats
		GlobalStats stats = result.getGlobalStats();
		System.out.println("\nGlobal statistics:");
		System.out.println("  Total documents: " + stats.getTotalDocs());
		System.out.println("  Documents containing terms:");

		Map<String, Integer> docsContainingTerm = stats.getDocsContainingTerm();
		if (docsContainingTerm != null)
		{
			for (Map.Entry<String, Integer> entry : docsContainingTerm.entrySet())
			{
				System.out.println("    " + entry.getKey() + ": " + entry.getValue() + " docs");
			}
		}

		// Print document results
		Map<String, QDocument> docs = result.getCandidateDocuments();
		System.out.println("\nFound " + (docs != null ? docs.size() : 0) + " candidate documents:");

		if (docs != null && !docs.isEmpty())
		{
			int count = 0;
			for (Map.Entry<String, QDocument> entry : docs.entrySet())
			{
				count++;
				QDocument doc = entry.getValue();
				System.out.println("\n" + count + ". Document: " + entry.getKey());
				System.out.println("   URL: " + doc.getMetadata().getUrl());

				Metadata metadata = doc.getMetadata();
				if (metadata != null)
				{
					System.out.println("   Popularity: " + metadata.getPopularity());
					System.out.println("   Length: " + metadata.getLength());
					System.out.println("   Publish date: " + metadata.getPublishDate());
				}

				// Print term stats for this document
				Map<String, TermStats> termStats = doc.getTermStats();
				if (termStats != null && !termStats.isEmpty())
				{
					System.out.println("   Term statistics:");
					for (Map.Entry<String, TermStats> termEntry : termStats.entrySet())
					{
						TermStats ts = termEntry.getValue();
						System.out.println("     " + termEntry.getKey() + ": tf=" + ts.getTf()
								+ ", inTitle=" + ts.isInTitle() + ", importance="
								+ ts.getImportanceScore());
					}
				}

				// Only show first 5 documents in detail to avoid too much output
				if (count >= 5 && docs.size() > 5)
				{
					System.out.println("\n...and " + (docs.size() - 5) + " more documents");
					break;
				}
			}
		}
		else
		{
			System.out.println("No matching documents found.");
		}
	}
}
