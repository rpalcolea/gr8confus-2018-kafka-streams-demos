package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

class FilteredOrdersExample {
    private static final ObjectMapper objectMapper = new ObjectMapper()

    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("order-filter-example")
        final StreamsBuilder builder = new StreamsBuilder()

        KStream<String, String> ordersStream = builder.stream("orders")
        KStream<String, String> ordersPerBook = ordersStream.filter({
            key, value -> objectMapper.readValue(value, Order).quantity > 5
        })
        ordersPerBook.to("filtered-orders")

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }
}
