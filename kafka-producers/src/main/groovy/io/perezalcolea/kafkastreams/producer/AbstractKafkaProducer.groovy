package io.perezalcolea.kafkastreams.producer

import groovyx.gpars.GParsPool
import io.perezalcolea.kafkastreams.message.KafkaMessage
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

abstract class AbstractKafkaProducer<T extends KafkaMessage> {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"

    abstract String getTopic()

    void produce(List<T> kafkaMessages) {
        KafkaProducer kafkaProducer = buildProducer()

        GParsPool.withPool {
            kafkaMessages.eachParallel { KafkaMessage kafkaMessage ->
                kafkaProducer.send(new ProducerRecord(getTopic(), kafkaMessage.messageKey, kafkaMessage.messageValue)).get()
            }
        }

        kafkaProducer.flush()
    }

    KafkaProducer buildProducer() {
        return new KafkaProducer([
                (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)     : DEFAULT_BOOTSTRAP_SERVERS,
                (ProducerConfig.ACKS_CONFIG)                  : "1",
                (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)  : "org.apache.kafka.common.serialization.StringSerializer",
                (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG): "org.apache.kafka.common.serialization.StringSerializer",
                (ProducerConfig.CLIENT_ID_CONFIG)             : "demo-producer"
        ])
    }
}
