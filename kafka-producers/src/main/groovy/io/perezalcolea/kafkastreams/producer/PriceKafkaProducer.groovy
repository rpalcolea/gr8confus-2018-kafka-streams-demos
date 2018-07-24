package io.perezalcolea.kafkastreams.producer

import io.perezalcolea.kafkastreams.message.PriceKafkaMessage

class PriceKafkaProducer extends AbstractKafkaProducer<PriceKafkaMessage> {
    @Override
    String getTopic() {
        return "book-prices"
    }
}
