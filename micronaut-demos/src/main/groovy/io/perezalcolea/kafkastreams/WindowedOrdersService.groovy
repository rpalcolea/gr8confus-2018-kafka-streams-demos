package io.perezalcolea.kafkastreams


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore

import javax.inject.Inject
import javax.inject.Singleton

@CompileStatic
@Slf4j
@Singleton
class WindowedOrdersService {

    private final KafkaStreams kafkaStreams

    WindowedOrdersService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams
    }

    List<Map<String, Long>> findLatestOrders(Long timeFrom, Long timeTo) {
        List<Map<String, Long>> results = []
        ReadOnlyWindowStore<String, Long> windowStore = kafkaStreams.store("orders-windowed-store", QueryableStoreTypes.windowStore())
        KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(timeFrom, timeTo)
        while(iterator.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iterator.next()
            results.add([(next.key.key()): next.value])
        }
        return results
    }
}
