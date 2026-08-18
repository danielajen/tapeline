package io.tapeline.serving.avro;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Fetches Avro schemas by their registry id.
 *
 * <p>Schema ids are immutable in a Confluent registry — a new schema always
 * gets a new id — so the cache never needs invalidation and the deserializer
 * makes exactly one network call per distinct schema per process lifetime.
 */
@Component
public class SchemaRegistryClient {

    private final RestClient http;
    private final Map<Integer, Schema> cache = new ConcurrentHashMap<>();

    public SchemaRegistryClient(@Value("${tapeline.schema-registry-url}") String baseUrl) {
        this.http = RestClient.builder().baseUrl(baseUrl.replaceAll("/+$", "")).build();
    }

    public Schema byId(int schemaId) {
        return cache.computeIfAbsent(schemaId, this::fetch);
    }

    private Schema fetch(int schemaId) {
        SchemaResponse response = http.get()
                .uri("/schemas/ids/{id}", schemaId)
                .retrieve()
                .body(SchemaResponse.class);

        if (response == null || response.schema() == null) {
            throw new IllegalStateException("schema registry returned no schema for id " + schemaId);
        }
        return new Schema.Parser().parse(response.schema());
    }

    /** Seeds the cache. Used by tests and by the local docker-compose profile,
     * where a registry may not be running. */
    public void preload(int schemaId, Schema schema) {
        cache.put(schemaId, schema);
    }

    record SchemaResponse(String schema) {}
}
