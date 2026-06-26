package com.displaypod;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class PaperMySqlSink extends RichSinkFunction<PaperEvent> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private transient Connection connection;

    public PaperMySqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
    }

    @Override
    public void invoke(PaperEvent paper, Context context) throws Exception {
        try {
            upsertPaper(paper);
            insertDimensions("paper_authors", "author_name", paper.source_id, paper.authors);
            insertDimensions("paper_institutions", "institution_name", paper.source_id, paper.institutions);
            insertDimensions("paper_keywords", "keyword", paper.source_id, paper.keywords);

            if (markFirstSeen(paper.source_id)) {
                incrementYear(paper.year);
                incrementStats("stats_authors", "author_name", paper.authors);
                incrementStats("stats_institutions", "institution_name", paper.institutions);
                incrementStats("stats_keywords", "keyword", paper.keywords);
            }
            connection.commit();
        } catch (Exception error) {
            connection.rollback();
            throw error;
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    private void upsertPaper(PaperEvent paper) throws SQLException {
        String sql = "INSERT INTO papers (source_id, year, title, abstract, paper_url, pdf_url, crawled_at) VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE year=VALUES(year), title=VALUES(title), abstract=VALUES(abstract), paper_url=VALUES(paper_url), pdf_url=VALUES(pdf_url), crawled_at=VALUES(crawled_at)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, paper.source_id);
            statement.setInt(2, paper.year);
            statement.setString(3, paper.title);
            statement.setString(4, paper.abstractText);
            statement.setString(5, paper.paper_url);
            statement.setString(6, paper.pdf_url);
            statement.setTimestamp(7, parseTimestamp(paper.crawled_at));
            statement.executeUpdate();
        }
    }

    private boolean markFirstSeen(String sourceId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT IGNORE INTO paper_ingest_events (paper_source_id) VALUES (?)")) {
            statement.setString(1, sourceId);
            return statement.executeUpdate() == 1;
        }
    }

    private void insertDimensions(String table, String valueColumn, String sourceId, List<String> values) throws SQLException {
        String sql = String.format("INSERT IGNORE INTO %s (paper_source_id, %s) VALUES (?, ?)", table, valueColumn);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (String value : values) {
                if (value == null || value.isBlank()) {
                    continue;
                }
                statement.setString(1, sourceId);
                statement.setString(2, value);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private void incrementYear(int year) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO stats_yearly (year, paper_count) VALUES (?, 1) ON DUPLICATE KEY UPDATE paper_count=paper_count+1")) {
            statement.setInt(1, year);
            statement.executeUpdate();
        }
    }

    private void incrementStats(String table, String keyColumn, List<String> values) throws SQLException {
        String sql = String.format(
                "INSERT INTO %s (%s, paper_count) VALUES (?, 1) ON DUPLICATE KEY UPDATE paper_count=paper_count+1",
                table,
                keyColumn
        );
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (String value : values) {
                if (value == null || value.isBlank()) {
                    continue;
                }
                statement.setString(1, value);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private Timestamp parseTimestamp(String value) {
        try {
            return Timestamp.from(Instant.parse(value));
        } catch (Exception ignored) {
            return Timestamp.from(Instant.now());
        }
    }
}
