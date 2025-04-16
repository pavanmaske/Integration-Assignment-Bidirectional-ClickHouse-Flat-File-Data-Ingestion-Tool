package com.example.service;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.example.model.ClickHouseConfig;
import com.example.model.IngestionConfig;
import com.example.model.TransferResponse;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class IngestionService {

    private static final String UPLOAD_DIR = "uploads";

    public void testConnection(ClickHouseConfig config) {
        try (Connection conn = getConnection(config)) {
            if (!conn.isValid(5)) {
                throw new RuntimeException("Failed to establish connection");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Connection test failed: " + e.getMessage());
        }
    }

    public String saveFile(MultipartFile file) {
        try {
            Path uploadPath = Paths.get(UPLOAD_DIR);
            if (!Files.exists(uploadPath)) {
                Files.createDirectories(uploadPath);
            }

            String fileName = UUID.randomUUID() + "-" + file.getOriginalFilename();
            Path filePath = uploadPath.resolve(fileName);
            Files.copy(file.getInputStream(), filePath);

            return filePath.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to save file: " + e.getMessage());
        }
    }

    public List<String> getTables(ClickHouseConfig config) {
        String sql = "SHOW TABLES";
        List<String> tables = new ArrayList<>();

        try (Connection conn = getConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get tables: " + e.getMessage());
        }

        return tables;
    }

    public List<String> getColumns(ClickHouseConfig config, String tableName) {
        String sql = String.format("DESCRIBE %s", tableName);
        List<String> columns = new ArrayList<>();

        try (Connection conn = getConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                columns.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get columns: " + e.getMessage());
        }

        return columns;
    }

    public List<String> getFileSchema(String filePath, String delimiter) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] header = reader.readNext();
            return header != null ? Arrays.asList(header) : Collections.emptyList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file schema: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> previewData(IngestionConfig config) {
        if (config.getSourceType().equals("CLICKHOUSE")) {
            return previewClickHouseData(config);
        } else {
            return previewFileData(config);
        }
    }

    private List<Map<String, Object>> previewClickHouseData(IngestionConfig config) {
        String columns = String.join(", ", config.getSelectedColumns());
        String tables = String.join(", ", config.getSelectedTables());
        String sql = String.format("SELECT %s FROM %s", columns, tables);

        if (config.getJoinCondition() != null && !config.getJoinCondition().isEmpty()) {
            sql += " WHERE " + config.getJoinCondition();
        }

        sql += " LIMIT " + config.getPreviewLimit();

        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection conn = getConnection(config.getClickHouseConfig());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to preview data: " + e.getMessage());
        }

        return results;
    }

    private List<Map<String, Object>> previewFileData(IngestionConfig config) {
        List<Map<String, Object>> results = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(config.getFlatFilePath()))) {
            String[] header = reader.readNext();
            if (header == null) return results;

            String[] nextLine;
            int count = 0;
            while ((nextLine = reader.readNext()) != null && count < config.getPreviewLimit()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < header.length; i++) {
                    if (config.getSelectedColumns().contains(header[i])) {
                        row.put(header[i], nextLine[i]);
                    }
                }
                results.add(row);
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to preview file data: " + e.getMessage());
        }

        return results;
    }

    public TransferResponse transfer(IngestionConfig config) {
        if (config.getSourceType().equals("CLICKHOUSE")) {
            return transferFromClickHouse(config);
        } else {
            return transferFromFile(config);
        }
    }

    private TransferResponse transferFromClickHouse(IngestionConfig config) {
        String columns = String.join(", ", config.getSelectedColumns());
        String tables = String.join(", ", config.getSelectedTables());
        String sql = String.format("SELECT %s FROM %s", columns, tables);

        if (config.getJoinCondition() != null && !config.getJoinCondition().isEmpty()) {
            sql += " WHERE " + config.getJoinCondition();
        }

        long recordCount = 0;
        String outputFile = Paths.get(UPLOAD_DIR, "export_" + UUID.randomUUID() + ".csv").toString();

        try (Connection conn = getConnection(config.getClickHouseConfig());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql);
             CSVWriter writer = new CSVWriter(new FileWriter(outputFile))) {

            // Write header
            writer.writeNext(config.getSelectedColumns().toArray(new String[0]));

            // Write data
            while (rs.next()) {
                String[] row = new String[config.getSelectedColumns().size()];
                for (int i = 0; i < config.getSelectedColumns().size(); i++) {
                    Object value = rs.getObject(config.getSelectedColumns().get(i));
                    row[i] = value != null ? value.toString() : "";
                }
                writer.writeNext(row);
                recordCount++;
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Failed to transfer data: " + e.getMessage());
        }

        return new TransferResponse("Data exported to " + outputFile, recordCount);
    }

    private TransferResponse transferFromFile(IngestionConfig config) {
        try (CSVReader reader = new CSVReader(new FileReader(config.getFlatFilePath()))) {
            String[] header = reader.readNext();
            if (header == null) {
                throw new RuntimeException("Empty file");
            }

            // Create table
            String tableName = "imported_" + UUID.randomUUID().toString().replace("-", "");
            String createTableSql = generateCreateTableSql(tableName, header);

            try (Connection conn = getConnection(config.getClickHouseConfig());
                 Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSql);

                // Prepare insert statement
                String insertSql = generateInsertSql(tableName, header);
                long recordCount = 0;

                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    String[] nextLine;
                    while ((nextLine = reader.readNext()) != null) {
                        for (int i = 0; i < header.length; i++) {
                            pstmt.setString(i + 1, nextLine[i]);
                        }
                        pstmt.addBatch();
                        recordCount++;

                        if (recordCount % 1000 == 0) {
                            pstmt.executeBatch();
                        }
                    }
                    pstmt.executeBatch();
                }

                return new TransferResponse("Data imported to table " + tableName, recordCount);
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException("Failed to transfer data: " + e.getMessage());
        }
    }

    private String generateCreateTableSql(String tableName, String[] columns) {
        return String.format("CREATE TABLE %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
                tableName,
                Arrays.stream(columns)
                        .map(col -> String.format("`%s` String", col))
                        .collect(Collectors.joining(", ")));
    }

    private String generateInsertSql(String tableName, String[] columns) {
        String columnList = Arrays.stream(columns)
                .map(col -> "`" + col + "`")
                .collect(Collectors.joining(", "));
        String valuePlaceholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, valuePlaceholders);
    }

    private Connection getConnection(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUsername());
        properties.setProperty("password", config.getJwtToken());
        return DriverManager.getConnection(config.getJdbcUrl(), properties);
    }
}