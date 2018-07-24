package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RestController

@CompileStatic
@RestController
@Slf4j
class InteractiveQueriesService {

    @Autowired
    StreamProcessor streamProcessor

    String findBook(String bookId) {
        ReadOnlyKeyValueStore<String, String> keyValueStore = streamProcessor.kafkaStreams.store(StreamProcessor.BOOKS_STORE, QueryableStoreTypes.keyValueStore())
        return keyValueStore.get(bookId)
    }

    String findPrice(String bookId) {
        ReadOnlyKeyValueStore<String, String> keyValueStore = streamProcessor.kafkaStreams.store(StreamProcessor.PRICES_STORE, QueryableStoreTypes.keyValueStore())
        return keyValueStore.get(bookId)
    }

    String findOrders(String bookId) {
        ReadOnlyKeyValueStore<String, String> keyValueStore = streamProcessor.kafkaStreams.store(StreamProcessor.ORDERS_STORE, QueryableStoreTypes.keyValueStore())
        return keyValueStore.get(bookId)
    }

    List<Map<String, Long>> findLatestOrders(Long timeFrom, Long timeTo) {
        List<Map<String, Long>> results = []
        ReadOnlyWindowStore<String, Long> windowStore = streamProcessor.kafkaStreams.store("orders-windowed-store", QueryableStoreTypes.windowStore())
        KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(timeFrom, timeTo)
        while(iterator.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iterator.next()
            results.add([(next.key.key()): next.value])
        }
        return results
    }
}
