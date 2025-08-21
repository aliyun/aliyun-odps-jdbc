# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a JDBC driver for MaxCompute (formerly ODPS), Alibaba Cloud's big data processing platform. The driver allows Java applications to connect to and query MaxCompute using standard JDBC APIs.

## Key Components

1. **Driver Registration**: `OdpsDriver` implements the JDBC Driver interface and registers itself with the DriverManager
2. **Connection Management**: `OdpsConnection` handles connection lifecycle and configuration
3. **Statement Execution**: Multiple Statement implementations (`OdpsStatement`, `OdpsPreparedStatement`, etc.) for executing SQL queries
4. **Result Handling**: Various ResultSet implementations for processing query results
5. **Data Type Transformation**: Utilities for converting between MaxCompute data types and JDBC/Java types

## Common Development Commands

### Build
```bash
# Build the project (skip tests)
./build.sh
# or
mvn clean package -DskipTests
```

### Run Tests
```bash
# Run all tests
mvn test

# Run a specific test class
mvn test -Dtest=OdpsStatementTest
```

## Project Structure

- `src/main/java/com/aliyun/odps/jdbc/` - Main JDBC driver implementation
- `src/main/java/com/aliyun/odps/jdbc/utils/` - Utility classes for data transformation, logging, etc.
- `src/test/java/com/aliyun/odps/jdbc/` - Unit tests
- `example/src/main/java/` - Example applications

## Key Configuration

Tests require a `conf.properties` file in `src/test/resources/` with connection details:
- `access_id` - Your Alibaba Cloud access key ID
- `access_key` - Your Alibaba Cloud access key secret
- `end_point` - The endpoint of your MaxCompute service
- `project_name` - The name of your MaxCompute project
- `logview_host` - The endpoint of MaxCompute Logview
- `charset` - Character set (default: UTF-8)

## Architecture Notes

- The driver uses the MaxCompute Java SDK as its underlying communication layer
- Connection parameters can be passed via URL or Properties object
- Supports both traditional mode and interactive mode (MCQA) for query execution
- Implements type conversion between MaxCompute types and standard JDBC types
- Uses SLF4J for logging with optional configuration file support