package io.perezalcolea.kafkastreams

import groovy.transform.CompileStatic
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Produces

import javax.inject.Inject

import static io.micronaut.http.HttpResponse.ok

@CompileStatic
@Controller("/")
class WindowedOrdersController {

    @Inject
    WindowedOrdersService windowedOrdersService

    @Get("/orders/latest")
    @Produces(MediaType.APPLICATION_JSON)
    HttpResponse<List<Map<String, Long>>> latestOrders() {
        long timeFrom = System.currentTimeMillis() - 60_000L
        long timeTo = System.currentTimeMillis()
        List<Map<String, Long>> orders = windowedOrdersService.findLatestOrders(timeFrom, timeTo)
        return ok(orders)
    }
}
