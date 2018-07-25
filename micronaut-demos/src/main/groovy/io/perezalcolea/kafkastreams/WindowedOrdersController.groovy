package io.perezalcolea.kafkastreams

import groovy.transform.CompileStatic
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Produces
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore

import static io.micronaut.http.HttpResponse.ok

@CompileStatic
@Controller("/")
class WindowedOrdersController {

    private final KafkaStreams kafkaStreams

    WindowedOrdersController(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams
    }

    @Get("/orders/latest")
    @Produces(MediaType.APPLICATION_JSON)
    HttpResponse<List<Map<String, Long>>> latestOrders() {
        long timeFrom = System.currentTimeMillis() - 60_000L
        long timeTo = System.currentTimeMillis()
        List<Map<String, Long>> orders = []
        ReadOnlyWindowStore<String, Long> windowStore = kafkaStreams.store("orders-windowed-store", QueryableStoreTypes.windowStore())
        KeyValueIterator<Windowed<String>, Long> iterator = windowStore.fetchAll(timeFrom, timeTo)
        while(iterator.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iterator.next()
            orders.add([(next.key.key()): next.value])
        }
        return ok(orders)
    }
}
