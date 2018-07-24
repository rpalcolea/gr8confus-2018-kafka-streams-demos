package io.perezalcolea.kafkastreams.message

import io.perezalcolea.kafkastreams.Book
import io.perezalcolea.kafkastreams.JsonWriter

class BookKafkaMessage implements KafkaMessage {

    private final Book book

    BookKafkaMessage(Book book) {
        this.book = book
    }

    @Override
    String getMessageKey() {
        return book.bookId
    }

    @Override
    String getMessageValue() {
        return JsonWriter.writeAsJsonString(book)
    }
}
