package com.learn.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class ElasticsearchSetup {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSetup.class);

    @Value("${elasticsearch.url:http://localhost:9200}")
    private String elasticsearchUrl;

    @Value("${elasticsearch.index:exchange_rates}")
    private String elasticsearchIndex;

    // Cette méthode s'exécute au démarrage de l'application
    @EventListener(ApplicationReadyEvent.class)
    public void setupElasticsearchIndex() {
        createIndexIfNotExists();
        checkElasticsearchConnection();
    }

    private void createIndexIfNotExists() {
        RestTemplate restTemplate = new RestTemplate();
        String indexUrl = elasticsearchUrl + "/" + elasticsearchIndex;

        try {
            // Vérifie si l'index existe déjà
            ResponseEntity<String> response = restTemplate.exchange(
                    indexUrl,
                    HttpMethod.HEAD,
                    null,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                log.info("L'index '{}' existe déjà dans Elasticsearch", elasticsearchIndex);
                return;
            }
        } catch (RestClientException e) {
            // L'index n'existe pas, on continue pour le créer
            log.info("L'index '{}' n'existe pas, création en cours...", elasticsearchIndex);
        }

        // Préparation de la définition de l'index avec mapping pour les taux de change
        String indexDefinition = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 1,\n" +
                "    \"number_of_replicas\": 0\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"base\": { \"type\": \"keyword\" },\n" +
                "      \"date\": { \"type\": \"date\" },\n" +
                "      \"time_last_updated\": { \"type\": \"long\" },\n" +
                "      \"rates\": { \"type\": \"object\" }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        // Configuration des headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(indexDefinition, headers);

        try {
            // Création de l'index
            ResponseEntity<String> response = restTemplate.exchange(
                    indexUrl,
                    HttpMethod.PUT,
                    entity,
                    String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Index '{}' créé avec succès : {}", elasticsearchIndex, response.getBody());
            } else {
                log.error("Échec de la création de l'index '{}'. Statut : {}", elasticsearchIndex, response.getStatusCode());
            }
        } catch (RestClientException e) {
            log.error("Erreur lors de la création de l'index '{}' : {}", elasticsearchIndex, e.getMessage());
        }
    }

    private void checkElasticsearchConnection() {
        RestTemplate restTemplate = new RestTemplate();

        try {
            // Vérification de base de la connexion Elasticsearch
            ResponseEntity<String> response = restTemplate.getForEntity(elasticsearchUrl, String.class);
            log.info("Connexion à Elasticsearch établie. Version : {}", response.getBody());

            // Vérification des documents dans l'index (utile pour les tests après quelques messages)
            String countUrl = elasticsearchUrl + "/" + elasticsearchIndex + "/_count";
            ResponseEntity<String> countResponse = restTemplate.getForEntity(countUrl, String.class);
            log.info("État de l'index '{}' : {}", elasticsearchIndex, countResponse.getBody());
        } catch (RestClientException e) {
            log.error("Impossible de se connecter à Elasticsearch : {}", e.getMessage());
        }
    }

    // Méthode utilitaire pour vérifier le contenu de l'index (à appeler manuellement ou périodiquement)
    public void checkIndexContent() {
        RestTemplate restTemplate = new RestTemplate();
        String searchUrl = elasticsearchUrl + "/" + elasticsearchIndex + "/_search?pretty";

        try {
            ResponseEntity<String> response = restTemplate.getForEntity(searchUrl, String.class);
            log.info("Contenu de l'index '{}' : {}", elasticsearchIndex, response.getBody());
        } catch (RestClientException e) {
            log.error("Erreur lors de la récupération du contenu de l'index : {}", e.getMessage());
        }
    }
}