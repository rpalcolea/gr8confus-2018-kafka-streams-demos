package io.perezalcolea.kafkastreams.message

interface KafkaMessage {
    String getMessageKey()
    String getMessageValue()
}