package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Produced

class BooksPerPublisherCountExample {

    private static final ObjectMapper objectMapper = new ObjectMapper()

    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("books-per-publisher-count-example")

        final StreamsBuilder builder = new StreamsBuilder()
        final KTable<String, Long> countByPublisher = builder.stream("books")
                .map(
                { String key, String json ->
                    Book book = objectMapper.readValue(json, Book)
                    return KeyValue.pair(book.publisher, book.title)
                })
                .groupBy({ key, value -> key })
                .count()


        countByPublisher.toStream().to("books-per-publisher", Produced.with(Serdes.String(), Serdes.Long()))

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }
}
