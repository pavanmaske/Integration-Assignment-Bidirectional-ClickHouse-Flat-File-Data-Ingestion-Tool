package com.example.controller;

import com.example.model.ClickHouseConfig;
import com.example.model.IngestionConfig;
import com.example.model.TransferResponse;
import com.example.service.IngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class IngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/test-connection")
    public ResponseEntity<Void> testConnection(@RequestBody ClickHouseConfig config) {
        ingestionService.testConnection(config);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/upload")
    public ResponseEntity<Map<String, String>> uploadFile(@RequestParam("file") MultipartFile file) {
        String filePath = ingestionService.saveFile(file);
        return ResponseEntity.ok(Map.of("filePath", filePath));
    }

    @PostMapping("/tables")
    public ResponseEntity<List<String>> getTables(@RequestBody ClickHouseConfig config) {
        List<String> tables = ingestionService.getTables(config);
        return ResponseEntity.ok(tables);
    }

    @PostMapping("/columns")
    public ResponseEntity<List<String>> getColumns(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName) {
        List<String> columns = ingestionService.getColumns(config, tableName);
        return ResponseEntity.ok(columns);
    }

    @PostMapping("/file-schema")
    public ResponseEntity<List<String>> getFileSchema(
            @RequestParam String filePath,
            @RequestParam(defaultValue = ",") String delimiter) {
        List<String> columns = ingestionService.getFileSchema(filePath, delimiter);
        return ResponseEntity.ok(columns);
    }

    @PostMapping("/preview")
    public ResponseEntity<List<Map<String, Object>>> previewData(@RequestBody IngestionConfig config) {
        List<Map<String, Object>> previewData = ingestionService.previewData(config);
        return ResponseEntity.ok(previewData);
    }

    @PostMapping("/transfer")
    public ResponseEntity<TransferResponse> transfer(@RequestBody IngestionConfig config) {
        TransferResponse response = ingestionService.transfer(config);
        return ResponseEntity.ok(response);
    }
}