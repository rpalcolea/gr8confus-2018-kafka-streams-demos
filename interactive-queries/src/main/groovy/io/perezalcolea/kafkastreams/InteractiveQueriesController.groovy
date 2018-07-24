package io.perezalcolea.kafkastreams

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@CompileStatic
@RestController
@Slf4j
class InteractiveQueriesController {

    @Autowired
    InteractiveQueriesService interactiveQueriesService

    @RequestMapping(path = "/book/{bookId}", produces = MediaType.APPLICATION_JSON_VALUE)
    String findBook(@PathVariable("bookId") String bookId) {
        return interactiveQueriesService.findBook(bookId)
    }

    @RequestMapping(path = "/price/{bookId}", produces = MediaType.APPLICATION_JSON_VALUE)
    String findPrice(@PathVariable("bookId") String bookId) {
        return interactiveQueriesService.findPrice(bookId)
    }

    @RequestMapping(path = "/orders/{bookId}", produces = MediaType.APPLICATION_JSON_VALUE)
    String findOrders(@PathVariable("bookId") String bookId) {
        return interactiveQueriesService.findOrders(bookId)
    }

    @RequestMapping(path = "/orders", produces = MediaType.APPLICATION_JSON_VALUE)
    List<Map<String, Long>> findOrdersByTime(@RequestParam("timeFrom") Long timeFrom, @RequestParam("timeTo") Long timeTo) {
        return interactiveQueriesService.findLatestOrders(timeFrom, timeTo)
    }

    @RequestMapping(path = "/orders/latest", produces = MediaType.APPLICATION_JSON_VALUE)
    List<Map<String, Long>> findLatestOrders() {
        long timeFrom = System.currentTimeMillis() - 60_000L
        long timeTo = System.currentTimeMillis()
        return interactiveQueriesService.findLatestOrders(timeFrom, timeTo)
    }
}
