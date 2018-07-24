package io.perezalcolea.kafkastreams


import io.perezalcolea.kafkastreams.message.PriceKafkaMessage
import io.perezalcolea.kafkastreams.producer.PriceKafkaProducer


class PriceKafkaProducerDemo {

    private static final Long DEFAULT_RANDOM_RECORDS_QUANTITY = 10000L

    static void main(String[] args) {
        PriceKafkaProducer producer = new PriceKafkaProducer()
        List<PriceKafkaMessage> messages = createMessages()
        println("SENDING $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS TO KAFKA")
        producer.produce(messages)
        println("SENT $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS TO KAFKA")
    }

    private static List<PriceKafkaMessage> createMessages() {
        List<PriceKafkaMessage> messages = []
        println("CREATING $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS")

        for (int i = 0; i < DEFAULT_RANDOM_RECORDS_QUANTITY; i++) {
            Price price = new Price()
            price.priceId = i + 1
            price.bookId = i + 1
            Double randomPrice = RandomDataGenerator.randomPrice()
            price.price = randomPrice
            price.formattedPrice = "\$$randomPrice"
            messages.add(new PriceKafkaMessage(price))
        }
        println("CREATED $DEFAULT_RANDOM_RECORDS_QUANTITY PRICE RECORDS")

        return messages
    }
}
