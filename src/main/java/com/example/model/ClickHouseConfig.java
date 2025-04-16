package com.example.model;

import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

@Data
public class ClickHouseConfig {
    @NotBlank(message = "Host is required")
    private String host;

    @NotNull(message = "Port is required")
    @Positive(message = "Port must be a positive number")
    private Integer port;

    @NotBlank(message = "Database name is required")
    private String database;

    @NotBlank(message = "Username is required")
    private String username;

    private String jwtToken;

    public String getJdbcUrl() {
        return String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
    }
}