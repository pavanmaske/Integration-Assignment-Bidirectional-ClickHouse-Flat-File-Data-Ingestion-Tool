package com.example.service;

import com.example.model.ClickHouseConfig;
import com.example.model.DataTransferConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.io.*;

@Slf4j
@Service
public class ClickHouseService {

    public Connection getConnection(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUsername());
        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("jwt", config.getJwtToken());
        }

        return DriverManager.getConnection(config.getJdbcUrl(), properties);
    }

    public List<String> getTables(ClickHouseConfig config) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection(config);
             ResultSet rs = conn.getMetaData().getTables(config.getDatabase(), null, null, new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    public List<String> getColumns(ClickHouseConfig config, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (Connection conn = getConnection(config);
             ResultSet rs = conn.getMetaData().getColumns(config.getDatabase(), null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    public Map<String, String> getColumnTypes(ClickHouseConfig config, String tableName) throws SQLException {
        Map<String, String> columnTypes = new HashMap<>();
        try (Connection conn = getConnection(config);
             ResultSet rs = conn.getMetaData().getColumns(config.getDatabase(), null, tableName, null)) {
            while (rs.next()) {
                columnTypes.put(
                    rs.getString("COLUMN_NAME"),
                    rs.getString("TYPE_NAME")
                );
            }
        }
        return columnTypes;
    }

    public List<Map<String, Object>> previewData(DataTransferConfig config) throws SQLException {
        List<Map<String, Object>> preview = new ArrayList<>();
        String query = buildSelectQuery(config);
        query += " LIMIT " + config.getPreviewLimit();

        try (Connection conn = getConnection(config.getClickHouseConfig());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                preview.add(row);
            }
        }
        return preview;
    }

    private String buildSelectQuery(DataTransferConfig config) {
        StringBuilder query = new StringBuilder("SELECT ");
        
        // Add selected columns
        query.append(String.join(", ", config.getSelectedColumns()));
        
        // Add FROM clause
        query.append(" FROM ").append(config.getSelectedTables().get(0));
        
        // Add JOIN clause if multiple tables are selected
        if (config.getSelectedTables().size() > 1 && config.getJoinCondition() != null) {
            for (int i = 1; i < config.getSelectedTables().size(); i++) {
                query.append(" JOIN ").append(config.getSelectedTables().get(i));
                query.append(" ON ").append(config.getJoinCondition());
            }
        }
        
        return query.toString();
    }

    public long exportToFile(DataTransferConfig config, String outputPath) throws SQLException, IOException {
        long recordCount = 0;
        String query = buildSelectQuery(config);

        try (Connection conn = getConnection(config.getClickHouseConfig());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {

            // Write header
            writer.write(String.join(config.getDelimiter(), config.getSelectedColumns()));
            writer.newLine();

            // Write data
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (String column : config.getSelectedColumns()) {
                    Object value = rs.getObject(column);
                    values.add(value != null ? value.toString() : "");
                }
                writer.write(String.join(config.getDelimiter(), values));
                writer.newLine();
                recordCount++;
            }
        }

        return recordCount;
    }
}