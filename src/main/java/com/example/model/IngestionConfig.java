package com.example.model;

import lombok.Data;
import java.util.List;

@Data
public class IngestionConfig {
    private String sourceType;
    private ClickHouseConfig clickHouseConfig;
    private String flatFilePath;
    private String delimiter;
    private List<String> selectedTables;
    private List<String> selectedColumns;
    private String joinCondition;
    private boolean previewMode;
    private int previewLimit;
}