package io.perezalcolea.kafkastreams

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder
import io.micronaut.context.annotation.Factory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows

import javax.inject.Singleton

@Factory
class WindowedOrdersStream {

    @Singleton
    KStream<String, String> windowedOrdersStream(ConfiguredStreamBuilder builder) {
        Properties props = builder.getConfiguration()
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") as KStream<String, String>

        KStream<String, String> source = builder.stream("orders")
        def ordersPerBook = source.groupByKey()
                .windowedBy(TimeWindows.of(60_000L))
                .count(Materialized.as("orders-windowed-store"))

        ordersPerBook.toStream().map({
            key, value ->  return KeyValue.pair(key.key(), value)
        }).to("orders-window-example", Produced.with(Serdes.String(), Serdes.Long()))

        return source
    }
}
