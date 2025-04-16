package com.example.service;

import com.example.model.ClickHouseConfig;
import com.example.model.DataTransferConfig;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Slf4j
@Service
public class FlatFileService {

    @Autowired
    private ClickHouseService clickHouseService;

    public List<String> discoverSchema(String filePath, String delimiter) throws IOException {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withSkipLines(0)
                .build()) {
            String[] headers = reader.readNext();
            return headers != null ? Arrays.asList(headers) : new ArrayList<>();
        }
    }

    public List<Map<String, String>> previewData(String filePath, String delimiter, int limit) throws IOException {
        List<Map<String, String>> preview = new ArrayList<>();
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withSkipLines(0)
                .build()) {

            String[] headers = reader.readNext();
            if (headers == null) return preview;

            String[] nextLine;
            int count = 0;
            while ((nextLine = reader.readNext()) != null && count < limit) {
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < headers.length && i < nextLine.length; i++) {
                    row.put(headers[i], nextLine[i]);
                }
                preview.add(row);
                count++;
            }
        }
        return preview;
    }

    public long importToClickHouse(DataTransferConfig config) throws SQLException, IOException {
        ClickHouseConfig clickHouseConfig = config.getClickHouseConfig();
        String tableName = config.getSelectedTables().get(0); // Target table name
        List<String> columns = config.getSelectedColumns();

        // Create temporary table with the same structure
        String tempTableName = tableName + "_temp_" + System.currentTimeMillis();
        String createTableQuery = generateCreateTableQuery(clickHouseConfig, tableName, tempTableName);

        try (Connection conn = clickHouseService.getConnection(clickHouseConfig);
             Statement stmt = conn.createStatement();
             BufferedReader reader = new BufferedReader(new FileReader(config.getFlatFilePath()))) {

            // Create temporary table
            stmt.execute(createTableQuery);

            // Skip header line
            reader.readLine();

            // Prepare insert query
            StringBuilder insertQuery = new StringBuilder()
                    .append("INSERT INTO ")
                    .append(tempTableName)
                    .append(" (")
                    .append(String.join(", ", columns))
                    .append(") VALUES");

            long recordCount = 0;
            List<String> batch = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(config.getDelimiter());
                if (values.length >= columns.size()) {
                    batch.add("('" + String.join("', '", Arrays.copyOf(values, columns.size())) + "')");
                    recordCount++;

                    // Execute batch insert
                    if (batch.size() >= 1000) {
                        executeBatchInsert(stmt, insertQuery.toString(), batch);
                        batch.clear();
                    }
                }
            }

            // Insert remaining records
            if (!batch.isEmpty()) {
                executeBatchInsert(stmt, insertQuery.toString(), batch);
            }

            // Move data from temporary table to target table
            String moveDataQuery = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                    tableName, String.join(", ", columns), String.join(", ", columns), tempTableName);
            stmt.execute(moveDataQuery);

            // Drop temporary table
            stmt.execute("DROP TABLE IF EXISTS " + tempTableName);

            return recordCount;
        }
    }

    private String generateCreateTableQuery(ClickHouseConfig config, String sourceTable, String tempTable) throws SQLException {
        Map<String, String> columnTypes = clickHouseService.getColumnTypes(config, sourceTable);
        StringBuilder createQuery = new StringBuilder()
                .append("CREATE TABLE ")
                .append(tempTable)
                .append(" (");

        List<String> columnDefinitions = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
            columnDefinitions.add(entry.getKey() + " " + entry.getValue());
        }

        createQuery.append(String.join(", ", columnDefinitions))
                .append(") ENGINE = Memory");

        return createQuery.toString();
    }

    private void executeBatchInsert(Statement stmt, String insertQuery, List<String> batch) throws SQLException {
        stmt.execute(insertQuery + " " + String.join(", ", batch));
    }
}