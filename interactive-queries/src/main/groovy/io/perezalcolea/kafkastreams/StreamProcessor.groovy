package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.util.logging.Slf4j
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Serialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import java.util.concurrent.TimeUnit

@Slf4j
@Component
class StreamProcessor {

    KafkaStreams kafkaStreams

    public static final String BOOKS_STORE = "books-store"
    public static final String PRICES_STORE = "prices-store"
    public static final String ORDERS_STORE = "orders-store"
    public static final String ORDERS_BY_BOOK_STORE = "orders-by-book-store"

    private static final ObjectMapper objectMapper = new ObjectMapper()

    @Autowired
    @Qualifier("streamProcessorProperties")
    Properties props

    @PostConstruct
    void init() {
        log.info("Initializing Stream app")
        kafkaStreams = createStream()
        kafkaStreams.setUncaughtExceptionHandler({ Thread thread, Throwable throwable ->
            log.error("STREAMS ERROR!!!! $thread", throwable)
            log.error("STREAMS ERROR!!!! ${throwable.stackTrace}", throwable)
            log.info("Closing Stream app")
            kafkaStreams.close(1, TimeUnit.MILLISECONDS)
            log.info("Restarting Stream app")
            init()
        })
        kafkaStreams.start()
    }

    @PreDestroy
    void shutdown() {
        kafkaStreams.close(10, TimeUnit.SECONDS)
    }

    KafkaStreams createStream() {
        final StreamsBuilder builder = new StreamsBuilder()
        Serde<String> stringSerde = Serdes.String()
        builder.table("books", Materialized.as(BOOKS_STORE))
        def table = builder.table("orders", Materialized.as(ORDERS_STORE))
        builder.table("book-prices", Materialized.as(PRICES_STORE))

        KStream<String, String> ordersStream = table.toStream()
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

        builder.table("orders-by-book", Materialized.as(ORDERS_BY_BOOK_STORE))

        KTable<Windowed<String>, Long> ordersPerBookWindowed = ordersStream.groupByKey()
                .windowedBy(TimeWindows.of(60_000L))
                .count(Materialized.as("orders-windowed-store"))

        ordersPerBookWindowed.toStream().map({
            key, value ->  return KeyValue.pair(key.key(), value)
        }).to("orders-by-windowed", Produced.with(Serdes.String(), Serdes.Long()))

        ordersPerBook.toStream().to("orders-by-book")

        return new KafkaStreams(builder.build(), props)
    }

    private List<String> deserializeValue(String value) {
        try {
            return objectMapper.readValue(value, List)
        } catch(Exception e) {
            return [value]
        }
    }

    private String serializeValue(List<String> values) {
        objectMapper.writeValueAsString(values)
    }
}
