package com.displaypod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class CvprPaperStreamJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BROKERS", "kafka:9092");
        String topic = env("KAFKA_TOPIC", "cvpr.raw.papers");
        String mysqlUrl = env("MYSQL_URL", "jdbc:mysql://mysql:3306/cvpr_display?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC");
        String mysqlUser = env("MYSQL_USER", "cvpr");
        String mysqlPassword = env("MYSQL_PASSWORD", "cvpr_pass");

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.enableCheckpointing(30_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("cvpr-paper-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<PaperEvent> papers = execution
                .fromSource(source, WatermarkStrategy.noWatermarks(), "cvpr-paper-source")
                .map(CvprPaperStreamJob::parsePaper)
                .filter(paper -> paper.source_id != null && !paper.source_id.isBlank() && paper.title != null && !paper.title.isBlank());

        papers.addSink(new PaperMySqlSink(mysqlUrl, mysqlUser, mysqlPassword)).name("paper-mysql-transactional-sink");

        execution.execute("CVPR Paper Stream Job");
    }

    private static PaperEvent parsePaper(String raw) throws Exception {
        JsonNode node = MAPPER.readTree(raw);
        PaperEvent paper = new PaperEvent();
        paper.source_id = text(node, "source_id");
        paper.year = node.path("year").asInt();
        paper.title = normalize(text(node, "title"));
        paper.abstractText = normalize(text(node, "abstract"));
        paper.paper_url = text(node, "paper_url");
        paper.pdf_url = text(node, "pdf_url");
        paper.crawled_at = text(node, "crawled_at");
        paper.authors = cleanList(node.path("authors"));
        paper.institutions = cleanList(node.path("institutions"));
        paper.keywords = cleanList(node.path("keywords"));
        return paper;
    }

    private static List<String> cleanList(JsonNode node) {
        Set<String> values = new LinkedHashSet<>();
        if (node.isArray()) {
            for (JsonNode item : node) {
                String value = normalize(item.asText(""));
                if (!value.isBlank()) {
                    values.add(value);
                }
            }
        }
        return new ArrayList<>(values);
    }

    private static String text(JsonNode node, String field) {
        return node.path(field).asText("");
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().replaceAll("\\s+", " ");
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
