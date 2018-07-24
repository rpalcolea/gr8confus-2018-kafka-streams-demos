package io.perezalcolea.kafkastreams

import io.perezalcolea.kafkastreams.message.OrderKafkaMessage
import io.perezalcolea.kafkastreams.producer.OrderKafkaProducer

class OrderKafkaProducerDemo {

    private static final Long DEFAULT_RANDOM_RECORDS_QUANTITY = 10000L

    static void main(String[] args) {
        OrderKafkaProducer producer = new OrderKafkaProducer()
        List<OrderKafkaMessage> messages = createMessages()
        println("SENDING $DEFAULT_RANDOM_RECORDS_QUANTITY ORDER RECORDS TO KAFKA")
        producer.produce(messages)
        println("SENT $DEFAULT_RANDOM_RECORDS_QUANTITY ORDER RECORDS TO KAFKA")
    }

    private static List<OrderKafkaMessage> createMessages() {
        List<OrderKafkaMessage> messages = []
        println("CREATING $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS")

        for (int i = 0; i < DEFAULT_RANDOM_RECORDS_QUANTITY; i++) {
            Order order = new Order()
            order.orderId = RandomDataGenerator.randomLong()
            order.bookId = i + 1
            Integer quantity = RandomDataGenerator.randomQuantity()
            order.quantity = quantity
            order.totalAmount = quantity * RandomDataGenerator.randomPrice()
            messages.add(new OrderKafkaMessage(order))
        }
        println("CREATED $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS")

        return messages
    }
}
