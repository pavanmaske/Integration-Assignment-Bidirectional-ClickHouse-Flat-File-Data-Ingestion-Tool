package com.example.controller;

import com.example.model.ClickHouseConfig;
import com.example.model.DataTransferConfig;
import com.example.service.ClickHouseService;
import com.example.service.FlatFileService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/ingestion")
@CrossOrigin(origins = "http://localhost:3000")
public class DataIngestionController {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FlatFileService flatFileService;

    @PostMapping("/test-connection")
    public ResponseEntity<?> testConnection(@RequestBody ClickHouseConfig config) {
        try {
            clickHouseService.getConnection(config);
            return ResponseEntity.ok().body(Map.of("message", "Connection successful"));
        } catch (Exception e) {
            log.error("Connection test failed", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/tables")
    public ResponseEntity<?> getTables(@RequestBody ClickHouseConfig config) {
        try {
            List<String> tables = clickHouseService.getTables(config);
            return ResponseEntity.ok(tables);
        } catch (Exception e) {
            log.error("Failed to get tables", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/columns")
    public ResponseEntity<?> getColumns(@RequestBody ClickHouseConfig config,
                                      @RequestParam String tableName) {
        try {
            List<String> columns = clickHouseService.getColumns(config, tableName);
            return ResponseEntity.ok(columns);
        } catch (Exception e) {
            log.error("Failed to get columns", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/preview")
    public ResponseEntity<?> previewData(@RequestBody DataTransferConfig config) {
        try {
            if ("CLICKHOUSE".equals(config.getSourceType())) {
                List<Map<String, Object>> preview = clickHouseService.previewData(config);
                return ResponseEntity.ok(preview);
            } else {
                List<Map<String, String>> preview = flatFileService.previewData(
                    config.getFlatFilePath(),
                    config.getDelimiter(),
                    config.getPreviewLimit()
                );
                return ResponseEntity.ok(preview);
            }
        } catch (Exception e) {
            log.error("Failed to preview data", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/file-schema")
    public ResponseEntity<?> getFileSchema(@RequestParam String filePath,
                                         @RequestParam(defaultValue = ",") String delimiter) {
        try {
            List<String> schema = flatFileService.discoverSchema(filePath, delimiter);
            return ResponseEntity.ok(schema);
        } catch (Exception e) {
            log.error("Failed to get file schema", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> transferData(@RequestBody DataTransferConfig config) {
        try {
            long recordCount;
            if ("CLICKHOUSE".equals(config.getSourceType())) {
                String outputPath = "export_" + System.currentTimeMillis() + ".csv";
                recordCount = clickHouseService.exportToFile(config, outputPath);
            } else {
                recordCount = flatFileService.importToClickHouse(config);
            }
            return ResponseEntity.ok(Map.of(
                "message", "Data transfer completed successfully",
                "recordCount", recordCount
            ));
        } catch (Exception e) {
            log.error("Data transfer failed", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/upload")
    public ResponseEntity<?> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            String fileName = file.getOriginalFilename();
            Path tempDir = Files.createTempDirectory("ingestion");
            Path filePath = tempDir.resolve(fileName);
            file.transferTo(filePath.toFile());
            
            return ResponseEntity.ok(Map.of(
                "message", "File uploaded successfully",
                "filePath", filePath.toString()
            ));
        } catch (Exception e) {
            log.error("File upload failed", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}