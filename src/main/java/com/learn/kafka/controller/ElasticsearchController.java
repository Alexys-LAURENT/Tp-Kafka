package com.learn.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/elasticsearch")
public class ElasticsearchController {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchController.class);

    @Value("${elasticsearch.url:http://localhost:9200}")
    private String elasticsearchUrl;

    @Value("${elasticsearch.index:exchange_rates}")
    private String elasticsearchIndex;

    @GetMapping("/status")
    public ResponseEntity<String> getElasticsearchStatus() {
        RestTemplate restTemplate = new RestTemplate();
        String response = restTemplate.getForObject(elasticsearchUrl, String.class);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/index/info")
    public ResponseEntity<String> getIndexInfo() {
        RestTemplate restTemplate = new RestTemplate();
        String url = elasticsearchUrl + "/" + elasticsearchIndex;
        try {
            String response = restTemplate.getForObject(url, String.class);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors de la récupération des informations de l'index : ", e);
            return ResponseEntity.status(500).body("Erreur: " + e.getMessage());
        }
    }

    @GetMapping("/data")
    public ResponseEntity<String> getIndexData() {
        RestTemplate restTemplate = new RestTemplate();
        String url = elasticsearchUrl + "/" + elasticsearchIndex + "/_search";
        try {
            String response = restTemplate.getForObject(url, String.class);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors de la récupération des données : ", e);
            return ResponseEntity.status(500).body("Erreur: " + e.getMessage());
        }
    }

    @GetMapping("/count")
    public ResponseEntity<String> getDocumentCount() {
        RestTemplate restTemplate = new RestTemplate();
        String url = elasticsearchUrl + "/" + elasticsearchIndex + "/_count";
        try {
            String response = restTemplate.getForObject(url, String.class);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors du comptage des documents : ", e);
            return ResponseEntity.status(500).body("Erreur: " + e.getMessage());
        }
    }
}