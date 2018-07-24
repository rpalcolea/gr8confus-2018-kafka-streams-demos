package io.perezalcolea.kafkastreams.message

import io.perezalcolea.kafkastreams.JsonWriter
import io.perezalcolea.kafkastreams.Order

class OrderKafkaMessage implements KafkaMessage {
    private final Order order

    OrderKafkaMessage(Order order) {
        this.order = order
    }

    @Override
    String getMessageKey() {
        return order.bookId
    }

    @Override
    String getMessageValue() {
        return JsonWriter.writeAsJsonString(order)
    }
}
