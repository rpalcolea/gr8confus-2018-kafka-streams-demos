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

class SalesPerBookExample {

    private static final ObjectMapper objectMapper = new ObjectMapper()

    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("sales-per-book-example")

        final StreamsBuilder builder = new StreamsBuilder()
        final KTable<String, Double> sumByBook = builder.stream("orders")
                .map(
                { String key, String json ->
                    Order order = objectMapper.readValue(json, Order)
                    return KeyValue.pair(key, order.totalAmount)
                })
                .groupBy({ key, value -> key }, Serialized.with(Serdes.String(), Serdes.Double()))
                .aggregate(
                { return 0.0d },
                { String key, Double price, Double totalAmount ->
                    return totalAmount + price
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        )


        sumByBook.toStream().to("total-sales-per-book", Produced.with(Serdes.String(), Serdes.Double()))

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }
}
