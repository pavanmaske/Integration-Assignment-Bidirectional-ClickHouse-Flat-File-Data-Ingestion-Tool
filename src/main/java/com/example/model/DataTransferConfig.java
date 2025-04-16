package com.example.model;

import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Data
public class DataTransferConfig {
    @NotBlank(message = "Source type is required")
    private String sourceType; // CLICKHOUSE or FLAT_FILE

    private ClickHouseConfig clickHouseConfig;

    private String flatFilePath;
    private String delimiter = ","; // Default delimiter for CSV files

    @NotEmpty(message = "At least one table must be selected")
    private List<String> selectedTables;

    @NotEmpty(message = "At least one column must be selected")
    private List<String> selectedColumns;

    // For bonus requirement: JOIN configuration
    private String joinCondition;
    private List<String> joinKeys;

    // For preview functionality
    private boolean previewMode;
    private int previewLimit = 100; // Default preview limit
}