package com.architect.poc.sinks;

import com.architect.poc.pipelines.EnterpriseEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GleanSecureSink extends RichSinkFunction<EnterpriseEvent> {
    private transient HttpClient httpClient;
    private transient ExecutorService pool;
    private String gleanUrl;
    private String token;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.gleanUrl = System.getenv("GLEAN_INSTANCE_URL");
        this.token = System.getenv("GLEAN_INDEXING_API_KEY");

        this.pool = Executors.newFixedThreadPool(16);
        this.httpClient = HttpClient.newBuilder()
                .executor(pool)
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public void invoke(EnterpriseEvent value, Context context) throws Exception {
        String targetEndpoint = String.format("%s/api/v1/indexdocument", gleanUrl);

        // Map conformed Flink object variables directly to Glean JSON footprint syntax
        String jsonPayload = String.format(
                "{\"id\":\"%s\",\"title\":\"Enriched Document\",\"body\":{\"mimeType\":\"text/plain\",\"textContent\":\"%s\"},\"permissions\":{\"allowedGroups\":[\"%s\"]}}",
                value.entityId,
                value.rawPayload.replace("\"", "\\\"").replace("\n", " "),
                String.join("\",\"", value.azureAdPermittedGroups)
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetEndpoint))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token)
                .header("X-Glean-Identity-Provider", "azure_ad")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        // High-velocity fire-and-forget async submission handling within task slots
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(res -> {
                    if (res.statusCode() != 200 && res.statusCode() != 202) {
                        System.err.println("Glean Secure Sink rejected transmission: " + res.statusCode());
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (pool != null) {
            pool.shutdown();
        }
    }
}