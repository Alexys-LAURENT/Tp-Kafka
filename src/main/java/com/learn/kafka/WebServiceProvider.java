package com.learn.kafka;

import com.learn.kafka.producer.MessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class WebServiceProvider {
    @Value("${spring.kafka.topic.name}")
    String topic;

    @Autowired
    private MessageProducer messageProducer;

    // Cette méthode s'exécutera automatiquement après le démarrage de l'application
    @EventListener(ApplicationReadyEvent.class)
    public void sendExchangeRatesOnStartup() {
        // Création d'une nouvelle instance de RestTemplate
        RestTemplate restTemplate = new RestTemplate();

        // Récupération des taux de change depuis l'API
        String exchangeRatesUrl = "https://api.exchangerate-api.com/v4/latest/USD";
        String exchangeRatesResponse = restTemplate.getForObject(exchangeRatesUrl, String.class);

        // Envoi des données au topic Kafka
        messageProducer.sendMessage(topic, exchangeRatesResponse);

        System.out.println("Exchange rates data sent to Kafka topic: " + topic);
    }
}