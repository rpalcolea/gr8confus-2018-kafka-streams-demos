package io.perezalcolea.kafkastreams.producer

import io.perezalcolea.kafkastreams.message.BookKafkaMessage

class BookKafkaProducer extends AbstractKafkaProducer<BookKafkaMessage> {
    @Override
    String getTopic() {
        return "books"
    }
}
