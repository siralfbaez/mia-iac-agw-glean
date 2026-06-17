package com.architect.poc.pipelines;

// --- Core Flink Connection & Stream Framework Imports ---
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// --- Custom POC Core Module Imports ---
import com.architect.poc.udfs.VertexAIAsyncEnricher;
import com.architect.poc.transformers.PayloadTransformer;
import com.architect.poc.sinks.GleanSecureSink;

// --- Base Java Standard Utilities ---
import java.util.ArrayList;
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
        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ConfluentCloudSource");

        // 2. Map Stream to Conformed Objects and Apply Business Rules Matrix
        DataStream<EnterpriseEvent> conformedStream = rawStream.map(json -> {
            EnterpriseEvent event = new EnterpriseEvent();
            event.sourceSystem = json.contains("channel") ? "slack" : (json.contains("changeEventHeader") ? "salesforce" : "teams");
            event.entityId = "id-" + java.util.UUID.randomUUID().toString().substring(0, 8);
            event.rawPayload = json;
            event.aclMetadata = new ArrayList<>();
            event.aclMetadata.add("entra-ad-group-placeholder-guid");
            return event;
        }).map(event -> {
            PayloadTransformer transformer = new PayloadTransformer();
            return transformer.transform(event);
        }).name("Enterprise-Payload-Transformation-Matrx");

        // 3. Apply Asynchronous Execution Graph to call Vertex AI non-blockingly
        DataStream<EnterpriseEvent> enrichedStream = AsyncDataStream.orderedWait(
                conformedStream,
                new VertexAIAsyncEnricher(), // Corrected name matching class reference
                11110, TimeUnit.MILLISECONDS,
                100
        ).name("Vertex-AI-Embedding-Async-Enrichment");

        // 4. Multiplex and Securely Dispatch to Downstream Sinks
        enrichedStream.addSink(new GleanSecureSink()).name("Glean-Secure-Identity-Sin");

        env.execute("MIA-IAC-AGW-Glean-SecureRouter");
    }
}