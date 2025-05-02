package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.example.Indexer.Tokenizer;
import com.example.QueryProcessor.QueryProcessor;
import com.example.Indexer.Stemmer;



import com.example.Indexer.StopWords;

@SpringBootApplication
public class SearchEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(SearchEngineApplication.class, args);
    }
    @Bean
    public Tokenizer tokenizer() {
        return new Tokenizer();
    }
    
    @Bean
    public Stemmer stemmer() {
        return new Stemmer();
    }

    @Bean
    public QueryProcessor queryProcessor() {
        return new QueryProcessor();
    }
    
    @Bean
    public StopWords stopWords() {
        return new StopWords();
    }
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                    .allowedOrigins("http://localhost:5173")
                    .allowedMethods("*")
                    .allowedHeaders("*")
                    .exposedHeaders("*")  // Add this line
                    .allowCredentials(true);  // If using cookies
            }
     };
    }
}