package com.example.controller;

import com.example.QueryProcessor.QueryProcessor;
import com.example.Ranker.RankedDocument;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/search")
@CrossOrigin(origins = {"http://localhost:5173", "http://127.0.0.1:5173"}) // Both localhost variants
public class SearchController {

    private final QueryProcessor queryProcessor;

    public SearchController(QueryProcessor queryProcessor) {
        this.queryProcessor = queryProcessor;
    }

    @GetMapping
    public ResponseEntity<List<RankedDocument>> search(
            @RequestParam String q,
            @RequestParam(defaultValue = "10") int limit
    ) {
        List<RankedDocument> results = queryProcessor.processQuery(q);
        return ResponseEntity.ok(results);
    }

    @PostMapping
    public ResponseEntity<List<RankedDocument>> searchPost(@RequestBody SearchRequest request) {
        List<RankedDocument> results = queryProcessor.processQuery(request.getQuery());
        return ResponseEntity.ok(results);
    }

    public static class SearchRequest {
        private String query;
        private int limit = 10;

        // Getters and setters
        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }
    }
}
