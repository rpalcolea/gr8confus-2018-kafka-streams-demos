package io.perezalcolea.kafkastreams

import com.fasterxml.jackson.databind.ObjectMapper

class JsonWriter {

    private static final ObjectMapper objectMapper = new ObjectMapper()

    static String writeAsJsonString(Object object) {
        return objectMapper.writeValueAsString(object)
    }
}
