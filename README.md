# Application Kafka - Taux de Change

Cette application Spring Boot démontre l'utilisation d'Apache Kafka pour le traitement en temps réel de données de taux de change, avec stockage dans Elasticsearch.

## Vue d'ensemble

L'application récupère automatiquement les taux de change depuis une API externe, les publie dans un topic Kafka, puis les consomme pour les stocker dans Elasticsearch. Elle fournit également des endpoints REST pour interroger les données stockées.

## Architecture

```
API Externe → Producer Kafka → Topic Kafka → Consumer Kafka → Elasticsearch
     ↓                                                              ↓
WebServiceProvider                                         ElasticsearchController
```

## Rôle du Producer et Consumer Kafka

### Producer Kafka : Publication des messages

#### Quand est-il appelé ?
Le **Producer** est activé automatiquement **au démarrage de l'application** via `WebServiceProvider`:

1. **Déclencheur** : `@EventListener(ApplicationReadyEvent.class)` - Se déclenche quand Spring Boot a terminé son initialisation
2. **Fréquence** : **Une seule fois** au démarrage (pas de répétition automatique)
3. **Action** : Récupère les taux de change de l'API externe et les publie dans Kafka

#### Comment ça fonctionne ?
```java
// WebServiceProvider.java - Séquence d'exécution :
1. Application Spring Boot démarre
2. @EventListener(ApplicationReadyEvent.class) se déclenche
3. sendExchangeRatesOnStartup() s'exécute automatiquement
4. RestTemplate appelle l'API : https://api.exchangerate-api.com/v4/latest/USD
5. messageProducer.sendMessage(topic, response) publie dans Kafka
```

#### Configuration du Producer
```java
// KafkaProducerConfig.java
- Bootstrap servers : localhost:9092
- Sérialisation : String → String (clé et valeur)
- Topic cible : "mon-tunnel-topic" (configurable via spring.kafka.topic.name)
```

### Consumer Kafka : Traitement des messages

#### Quand est-il appelé ?
Le **Consumer** fonctionne en **mode écoute permanente** :

1. **Déclencheur** : `@KafkaListener` - Écoute en continu le topic configuré
2. **Fréquence** : **À chaque nouveau message** publié dans le topic
3. **Mode** : **Asynchrone** - Traite les messages dès leur arrivée

#### Comment ça fonctionne ?
```java
// MessageConsumer.java - Cycle de traitement :
1. @KafkaListener surveille en permanence "mon-tunnel-topic"
2. Dès qu'un message arrive → listen(String message) s'exécute
3. Log du message reçu
4. sendToElasticsearch(message) envoie vers Elasticsearch
5. Retour en mode écoute pour le prochain message
```

#### Configuration du Consumer
```java
// KafkaConsumerConfig.java
- Bootstrap servers : localhost:9092
- Group ID : "tp-kafka-step1" (permet la répartition de charge)
- Désérialisation : String → String
- Auto-commit : activé par défaut
```

### Cycle de vie complet

```
DÉMARRAGE APPLICATION
        ↓
1. Spring Boot initialise tous les beans
        ↓
2. Consumer démarre et écoute le topic "mon-tunnel-topic"
        ↓
3. ApplicationReadyEvent se déclenche
        ↓
4. WebServiceProvider récupère les données API
        ↓
5. Producer publie dans "mon-tunnel-topic"
        ↓
6. Consumer reçoit IMMÉDIATEMENT le message
        ↓
7. Consumer traite et envoie vers Elasticsearch
        ↓
8. Consumer retourne en mode écoute (permanent)
```

### Points importants

#### Producer
- **Synchrone** : `kafkaTemplate.send()` est bloquant
- **Une seule exécution** au démarrage
- **Pas de retry automatique** en cas d'erreur
- **Sérialise en String** : Le JSON reste au format texte

#### Consumer
- **Asynchrone** : Traitement en arrière-plan
- **Écoute permanente** : Ne s'arrête jamais
- **Gestion d'erreur** : Try-catch pour éviter que l'application crash
- **Group ID** : Permet d'avoir plusieurs instances qui se répartissent les messages

#### Pourquoi cette architecture ?
1. **Découplage** : Producer et Consumer sont indépendants
2. **Résilience** : Si Elasticsearch tombe, les messages restent dans Kafka
3. **Scalabilité** : Possibilité d'ajouter plusieurs consumers avec le même group-id
4. **Traçabilité** : Tous les messages transitent par Kafka (audit trail)

## Fonctionnalités principales

- **Récupération automatique** : Les taux de change sont récupérés depuis l'API Exchange Rate API au démarrage
- **Production Kafka** : Publication des données dans un topic Kafka configuré
- **Consommation Kafka** : Écoute du topic et traitement des messages
- **Stockage Elasticsearch** : Persistance des données pour analyse et recherche
- **API REST** : Endpoints pour consulter les données et l'état d'Elasticsearch

## Structure du projet

### Configuration

#### `application.properties`
```properties
# Configuration Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=tp-kafka-step1
spring.kafka.topic.name=mon-tunnel-topic

# Configuration Elasticsearch
elasticsearch.url=http://localhost:9200
elasticsearch.index=exchange_rates
```

### Packages et classes

#### `com.learn.kafka.producer`
- **`KafkaProducerConfig`** : Configuration du producteur Kafka avec sérialisation String
- **`MessageProducer`** : Service pour publier des messages vers un topic Kafka

#### `com.learn.kafka.consumer`
- **`KafkaConsumerConfig`** : Configuration du consommateur Kafka avec désérialisation String
- **`MessageConsumer`** : Listener Kafka qui traite les messages et les envoie vers Elasticsearch

#### `com.learn.kafka.config`
- **`ElasticsearchSetup`** : Configuration automatique de l'index Elasticsearch au démarrage

#### `com.learn.kafka.controller`
- **`ElasticsearchController`** : API REST pour interroger Elasticsearch
  - `GET /api/elasticsearch/status` : État d'Elasticsearch
  - `GET /api/elasticsearch/index/info` : Informations sur l'index
  - `GET /api/elasticsearch/data` : Données stockées
  - `GET /api/elasticsearch/count` : Nombre de documents

#### `com.learn.kafka`
- **`WebServiceProvider`** : Service qui récupère les données de l'API externe au démarrage
- **`KafkaApplication`** : Classe principale Spring Boot

## Flux de données

1. **Au démarrage** : `WebServiceProvider` récupère les taux de change depuis `https://api.exchangerate-api.com/v4/latest/USD`
2. **Production** : Les données JSON sont publiées dans le topic Kafka configuré
3. **Consommation** : `MessageConsumer` écoute le topic et reçoit les messages
4. **Stockage** : Les données sont automatiquement envoyées vers Elasticsearch
5. **Consultation** : Les endpoints REST permettent d'interroger les données stockées

## Prérequis

- Java 23
- Apache Kafka (localhost:9092)
- Elasticsearch (localhost:9200)
- Maven 3.9+

## Installation et démarrage

### 1. Démarrer les services externes

```bash
# Kafka
bin/kafka-server-start.sh config/server.properties

# Elasticsearch
bin/elasticsearch
```

### 2. Créer le topic Kafka

```bash
bin/kafka-topics.sh --create --topic mon-tunnel-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Lancer l'application

```bash
./mvnw spring-boot:run
```

## Utilisation

### Endpoints disponibles

- **Status Elasticsearch** : `GET http://localhost:8080/api/elasticsearch/status`
- **Info index** : `GET http://localhost:8080/api/elasticsearch/index/info`
- **Données** : `GET http://localhost:8080/api/elasticsearch/data`
- **Comptage** : `GET http://localhost:8080/api/elasticsearch/count`

### Surveillance

L'application utilise Spring Boot Actuator avec tous les endpoints exposés :
- Health check : `GET http://localhost:8080/actuator/health`
- Métriques : `GET http://localhost:8080/actuator/metrics`

## Format des données

Les données de taux de change suivent ce format JSON :
```json
{
  "base": "USD",
  "date": "2025-06-26",
  "time_last_updated": 1719360000,
  "rates": {
    "EUR": 0.85,
    "GBP": 0.73,
    "JPY": 110.0
  }
}
```

## Configuration Elasticsearch

L'index `exchange_rates` est créé automatiquement avec le mapping suivant :
- `base` : keyword (devise de base)
- `date` : date
- `time_last_updated` : long (timestamp)
- `rates` : object (taux de change)

## Logs

L'application utilise SLF4J pour le logging. Les messages importants incluent :
- Réception de messages Kafka
- Envoi vers Elasticsearch
- Erreurs de traitement
- État de la connexion Elasticsearch

## Développement

Cette application est conçue comme un kata/exercice pour apprendre :
- L'intégration Spring Boot + Kafka
- La configuration des producers/consumers Kafka
- L'utilisation d'Elasticsearch comme système de stockage
- Les patterns de microservices avec messaging

## Dépendances principales

- Spring Boot 3.4.2
- Spring Kafka
- Spring Web
- Spring Boot Actuator
- Lombok (optionnel)