package io.perezalcolea.kafkastreams

import io.perezalcolea.kafkastreams.message.BookKafkaMessage
import io.perezalcolea.kafkastreams.producer.BookKafkaProducer
import io.github.benas.randombeans.EnhancedRandomBuilder
import io.github.benas.randombeans.api.EnhancedRandom
import io.github.benas.randombeans.randomizers.range.LongRangeRandomizer

import static io.github.benas.randombeans.FieldDefinitionBuilder.field

class BookKafkaProducerDemo {

    private static final Long DEFAULT_RANDOM_RECORDS_QUANTITY = 10000L

    private static final EnhancedRandom enhancedRandom = EnhancedRandomBuilder.aNewEnhancedRandomBuilder()
            .stringLengthRange(10, 20)
            .randomize(Long.class, new LongRangeRandomizer(1L, DEFAULT_RANDOM_RECORDS_QUANTITY))
            .exclude(field().named("bookId").ofType(Long.class).inClass(Book.class).get())
            .exclude(field().named("publisher").ofType(String.class).inClass(Book.class).get())
            .exclude(field().named("language").ofType(String.class).inClass(Book.class).get())
            .build()

    static void main(String[] args) {
        BookKafkaProducer producer = new BookKafkaProducer()
        List<BookKafkaMessage> messages = createMessages()
        println("SENDING $DEFAULT_RANDOM_RECORDS_QUANTITY BOOK RECORDS TO KAFKA")
        producer.produce(messages)
        println("SENT $DEFAULT_RANDOM_RECORDS_QUANTITY BOOK RECORDS TO KAFKA")
    }

    private static List<BookKafkaMessage> createMessages() {
        List<BookKafkaMessage> messages = []
        println("CREATING $DEFAULT_RANDOM_RECORDS_QUANTITY BOOK RECORDS")

        for (int i = 0; i < DEFAULT_RANDOM_RECORDS_QUANTITY; i++) {
            Book book = enhancedRandom.nextObject(Book.class)
            book.bookId = i + 1
            book.language = RandomDataGenerator.randomLanguage()
            book.publisher = RandomDataGenerator.randomPublisher()
            messages.add(new BookKafkaMessage(book))
        }
        println("CREATED $DEFAULT_RANDOM_RECORDS_QUANTITY BOOK RECORDS")

        return messages
    }
}
