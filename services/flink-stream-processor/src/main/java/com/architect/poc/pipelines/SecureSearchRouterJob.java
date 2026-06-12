package com.architect.poc.pipelines;

// --- required imports ---
import com.architect.poc.udfs.VertexAIAsyncEnricher;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SecureSearchRouterJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Enforce Security Configs for Confluent Cloud Connection ---
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", System.getenv("CONFLUENT_BOOTSTRAP_SERVERS"));
        kafkaProps.put("security.protocol", "SSL");
        kafkaProps.put("ssl.enabled.protocols", "TLSv1.3");
        kafkaProps.put("ssl.truststore.location", System.getenv("SSL_TRUSTSTORE_LOCATION"));
        kafkaProps.put("ssl.truststore.password", System.getenv("SSL_TRUSTSTORE_PASSWORD"));
        kafkaProps.put("ssl.keystore.location", System.getenv("SSL_KEYSTORE_LOCATION"));
        kafkaProps.put("ssl.keystore.password", System.getenv("SSL_KEYSTORE_PASSWORD"));
        kafkaProps.put("ssl.key.password", System.getenv("SSL_KEY_PASSWORD"));

        // Define secure unified Ingestion Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics("unified-enterprise-mutations")
                .setGroupId("mia-flink-search-router")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 1. Read Raw Stream
        DataStream<String> rawStream = env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "ConfluentCloudSource");

        // 2. Map Stream to Conformed Objects
        DataStream<EnterpriseEvent> conformedStream = rawStream.map(json -> {
            // In implementation, map incoming json string to EnterpriseEvent object
            EnterpriseEvent event = new EnterpriseEvent();
            event.sourceSystem = "slack";
            event.rawPayload = json;
            return event;
        });

        // 3. Apply Asynchronous Execution Graph to call Vertex AI non-blockingly
        // Setting a 10-second timeout with a capacity limit of 100 concurrent async calls
        DataStream<EnterpriseEvent> enrichedStream = AsyncDataStream.unorderedWait(
                conformedStream,
                new VertexAIAsyncEnricher(),
                10000, TimeUnit.MILLISECONDS,
                100
        ).name("VertexAI-Async-Enrichment-Worker");

        // 4. Multiplex and Route to Sinks
        // Sink A: Send everything to Glean with strict Azure AD permissions attached
        enrichedStream.print(); // Placeholder for Glean Custom Push Sink Node

        env.execute("MIA-IAC-AGW-Glean-SecureRouter");
    }
}