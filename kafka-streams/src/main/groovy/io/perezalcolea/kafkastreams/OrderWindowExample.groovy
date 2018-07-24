package io.perezalcolea.kafkastreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows

class OrderWindowExample {
    static void main(final String[] args) throws Exception {
        Properties streamsConfiguration = KafkaStreamsConfig.getConfig("order-window-example")

        final StreamsBuilder builder = new StreamsBuilder()
        KStream<String, String> ordersStream = builder.stream("orders")
        def ordersPerBook = ordersStream.groupByKey()
                .windowedBy(TimeWindows.of(60_000L))
                .count()

        ordersPerBook.toStream().map({
            key, value ->  return KeyValue.pair(key.key(), value)
        }).to("orders-window-example", Produced.with(Serdes.String(), Serdes.Long()))

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration)
        streams.cleanUp()
        streams.start()
    }
}
