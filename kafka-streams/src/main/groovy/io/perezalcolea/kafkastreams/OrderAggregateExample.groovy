package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Serialized

class OrderAggregateExample {
    private static final ObjectMapper objectMapper = new ObjectMapper()

    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("order-aggregate-example")
        Serde<String> stringSerde = Serdes.String()

        final StreamsBuilder builder = new StreamsBuilder()
        KStream<String, String> ordersStream = builder.stream("orders")
        KTable<String, String> ordersPerBook = ordersStream.groupByKey(Serialized.with(stringSerde, stringSerde))
                .aggregate(
                { return objectMapper.writeValueAsString([]) },
                { String key, String order, String agg ->
                    List<String> orders = deserializeValue(agg)
                    orders.add(order)
                    return serializeValue(orders)
                },
                Materialized.with(stringSerde, stringSerde)
        )

        ordersPerBook.toStream().to("orders-aggregate-example")

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }

    private static List<String> deserializeValue(String value) {
        try {
            return objectMapper.readValue(value, List)
        } catch(Exception e) {
            return [value]
        }
    }

    private static String serializeValue(List<String> values) {
        objectMapper.writeValueAsString(values)
    }
}
