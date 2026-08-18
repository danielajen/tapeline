package io.tapeline.serving.config;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/** Beans that need explicit wiring rather than auto-configuration. */
@Configuration
public class ServingConfig {

    /**
     * Two data sources: Postgres for API keys and symbol metadata, and the
     * OLAP store for window queries. They are separate because their failure
     * modes should be: a slow analytical query must not exhaust the pool that
     * authentication depends on, or a heavy reporting query would take the
     * whole API down with it.
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSource metadataDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties("tapeline.olap.datasource")
    public DataSource olapDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(@Qualifier("metadataDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public JdbcTemplate olapJdbcTemplate(
            @Qualifier("olapDataSource") DataSource ds,
            @Value("${tapeline.olap.query-timeout-seconds:5}") int queryTimeoutSeconds) {
        JdbcTemplate template = new JdbcTemplate(ds);
        // A query timeout, not a suggestion. Without it, one pathological
        // range scan holds a connection until the client gives up, and a
        // handful of those exhaust the pool.
        template.setQueryTimeout(queryTimeoutSeconds);
        return template;
    }

    /**
     * Byte-array Kafka consumer.
     *
     * <p>Values stay as raw bytes because the Confluent wire header has to be
     * parsed before the payload means anything — see ConfluentAvroReader.
     * Batch mode, because per-record listener dispatch is measurable overhead
     * at quote volumes and the handler is cheap.
     */
    @Bean
    public ConsumerFactory<String, byte[]> byteArrayConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${tapeline.kafka.max-poll-records:500}") int maxPollRecords) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        // The stream tier writes quotes inside Kafka transactions. Without
        // read_committed this consumer would see aborted records, and the
        // exactly-once guarantee that costs the stream tier its latency
        // budget would buy nothing at all.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
            byteArrayKafkaListenerContainerFactory(
                    ConsumerFactory<String, byte[]> byteArrayConsumerFactory,
                    @Value("${tapeline.kafka.concurrency:3}") int concurrency) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(byteArrayConsumerFactory);
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true);
        return factory;
    }
}
