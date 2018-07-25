# gr8confus-2018-kafka-streams-demos
Demos for my presentation **High Scalable Streaming Microservices with Kafka Streams** at GR8ConfUS 2018 (http://gr8conf.us/talks/670).

Kafka Streams is a library for building streaming applications, specifically applications that transform input Kafka topics into output Kafka topics (or calls to external services, or updates to databases, or exposing the data via HTTP). Kafka Streams API allows to embed stream processing into any application or microservice to add lightweight but scalable and mission-critical stream processing logic.

For slides please refer to https://github.com/rpalcolea/gr8confus-2018-presentations/blob/master/kafka-streams-presentation.pdf

# Demo

## Pre-requisites

* Docker
* Kafka console consumer

### Running docker-compose file

`docker-compose up`

This will initialize a kafka broker with the required topics

If you need to customize the compose file, please refer to https://github.com/wurstmeister/kafka-docker.

### Downloading kafka-console-consumer

* Download latest kafka release from https://www.apache.org/dyn/closer.cgi?path=/kafka/1.1.0/kafka_2.11-1.1.0.tgz
* Untar the downloaded file
* You will find `kafka-console-consumer.sh` in `kafka_2.11-1.1.0/bin`

## Producing messages in topics

### Books 

```
./gradlew kafka-producers:execute -PmainClass=io.perezalcolea.kafkastreams.BookKafkaProducerDemo
```

### Orders

```
./gradlew kafka-producers:execute -PmainClass=io.perezalcolea.kafkastreams.OrderKafkaProducerDemo
```

### Prices

```
./gradlew kafka-producers:execute -PmainClass=io.perezalcolea.kafkastreams.PriceKafkaProducerDemo
```

## Consuming messages from topics

### Books

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic books --property print.key=true --from-beginning
```

### Orders

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --property print.key=true --from-beginning
```

### Prices

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic book-prices --property print.key=true --from-beginning
```

## Kafka Streams

### Filter orders by quantity

`FilteredOrdersExample` creates a stream from `orders` topic and filters  messages where quantity of a book is > 5.

The results are written into `filtered-orders` topic.

Executing Kafka Streams app

```
./gradlew kafka-streams:execute -PmainClass=io.perezalcolea.kafkastreams.FilteredOrdersExample
```

Reading messages from topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic filtered-orders --property print.key=true --from-beginning
```

### Count books per publisher

`BooksPerPublisherCountExample` creates a stream from `books` topic and counts them per publisher.

The results are written into `books-per-publisher` topic.

Executing Kafka Streams app

```
./gradlew kafka-streams:execute -PmainClass=io.perezalcolea.kafkastreams.BooksPerPublisherCountExample
```

Reading messages from topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic books-per-publisher --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --from-beginning
```

### Total sold books

`SoldBooksExample` creates a stream from `orders` topic and counts sold books.

The results are written into `total-sold-books` topic.

Executing Kafka Streams app

```
./gradlew kafka-streams:execute -PmainClass=io.perezalcolea.kafkastreams.SoldBooksExample
```

Reading messages from topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic total-sold-books --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer --from-beginning
```

### Total sales per book

`SalesPerBookExample` creates a stream from `orders` topic and sums the total sales per book.

The results are written into `total-sales-per-book` topic.

Executing Kafka Streams app

```
./gradlew kafka-streams:execute -PmainClass=io.perezalcolea.kafkastreams.SalesPerBookExample
```

Reading messages from topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic total-sales-per-book --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer --from-beginning
```

### Orders in the last minute

`OrderWindowExample` creates a windoed stream from `orders` topic grouping orders by book in the last minute.

The results are written into `orders-window-example` topic.

Executing Kafka Streams app

```
./gradlew kafka-streams:execute -PmainClass=io.perezalcolea.kafkastreams.OrderWindowExample
```

Reading messages from topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-window-example --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --from-beginning
```

* Note: You need to run the orders producer multiple times in order to see new orders coming. This is expected since we are doing a 1 minute window.

## Kafka Streams - Interactive Queries

Spring Boot app that allows to expose Kafka Streams KTable via HTTP endpoint.

### Executing the app

```
./gradlew :interactive-queries:bootRun
```

### Retrieving data

#### Book

```
curl "http://localhost:8080/book/42"
```

#### Price

```
curl "http://localhost:8080/price/42"
```

#### Order

```
curl "http://localhost:8080/orders/42"
```

#### Latest Orders (1 minute)

```
curl "http://localhost:8080/orders/latest"
```

* Note: You need to run the orders producer multiple times in order to see new orders coming. This is expected since we are doing a 1 minute window.

## Kafka Streams - Interactive Queries - Micronaut

Micronaut is a modern, JVM-based, full stack microservices framework designed for building modular, easily testable microservice applications.

Micronaut is developed by the creators of the Grails framework and takes inspiration from lessons learnt over the years building real-world applications from monoliths to microservices using Spring, Spring Boot and Grails.

### Executing the app

```
./gradlew :micronaut-demos:run
```

### Retrieving data

#### Latest Orders (1 minute)

```
curl "http://localhost:8080/orders/latest"
```

* Note: You need to run the orders producer multiple times in order to see new orders coming. This is expected since we are doing a 1 minute window.


# Useful links

[https://docs.confluent.io/current/streams/index.html](https://docs.confluent.io/current/streams/index.html)
[https://kafka.apache.org/documentation/](https://kafka.apache.org/documentation/)