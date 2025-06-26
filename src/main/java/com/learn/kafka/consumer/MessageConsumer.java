package com.learn.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @Value("${elasticsearch.url:http://localhost:9200}")
    private String elasticsearchUrl;

    @Value("${elasticsearch.index:exchange_rates}")
    private String elasticsearchIndex;

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String message) {
        log.info("Message reçu : {}", message);

        try {
            // Envoi des données à Elasticsearch
            sendToElasticsearch(message);
        } catch (Exception e) {
            log.error("Erreur lors de l'envoi à Elasticsearch : ", e);
        }
    }

    private void sendToElasticsearch(String message) {
        RestTemplate restTemplate = new RestTemplate();

        // Construction de l'URL pour l'index Elasticsearch
        // Format : http://localhost:9200/index_name/_doc
        String url = elasticsearchUrl + "/" + elasticsearchIndex + "/_doc";

        // Configuration des headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // Création de l'entité HTTP avec le message et un timestamp
        HttpEntity<String> request = new HttpEntity<>(message, headers);

        // Envoi de la requête à Elasticsearch
        String response = restTemplate.postForObject(url, request, String.class);

        log.info("Données envoyées à Elasticsearch: {}", response);
    }
}