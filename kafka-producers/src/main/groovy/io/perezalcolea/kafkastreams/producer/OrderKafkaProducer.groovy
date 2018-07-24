package io.perezalcolea.kafkastreams.producer

import io.perezalcolea.kafkastreams.message.OrderKafkaMessage

class OrderKafkaProducer extends AbstractKafkaProducer<OrderKafkaMessage> {
    @Override
    String getTopic() {
        return "orders"
    }
}
