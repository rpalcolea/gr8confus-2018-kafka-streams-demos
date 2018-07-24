package io.perezalcolea.kafkastreams.message

import io.perezalcolea.kafkastreams.JsonWriter
import io.perezalcolea.kafkastreams.Price

class PriceKafkaMessage implements KafkaMessage {
    private final Price price

    PriceKafkaMessage(Price price) {
        this.price = price
    }

    @Override
    String getMessageKey() {
        return price.bookId
    }

    @Override
    String getMessageValue() {
        return JsonWriter.writeAsJsonString(price)
    }
}
