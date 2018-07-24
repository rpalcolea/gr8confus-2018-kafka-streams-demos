package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Serialized

class SoldBooksExample {

    private static final ObjectMapper objectMapper = new ObjectMapper()

    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("sold-books-example")

        final StreamsBuilder builder = new StreamsBuilder()
        final KTable<String, Integer> soldByBook = builder.stream("orders")
                .map(
                { String key, String json ->
                    Order order = objectMapper.readValue(json, Order)
                    return KeyValue.pair(key, order.quantity)
                })
                .groupBy({ key, value -> key }, Serialized.with(Serdes.String(), Serdes.Integer()))
                .aggregate(
                { return 0 },
                { String key, Integer quantity, Integer totalSold ->
                    return totalSold + quantity
                },
                Materialized.with(Serdes.String(), Serdes.Integer())
        )


        soldByBook.toStream().to("total-sold-books", Produced.with(Serdes.String(), Serdes.Integer()))

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }
}
