package io.perezalcolea.kafkastreams

class Order {
    Long orderId
    Long bookId
    Long purchaseDate = System.currentTimeMillis()
    Integer quantity
    Double totalAmount
}
