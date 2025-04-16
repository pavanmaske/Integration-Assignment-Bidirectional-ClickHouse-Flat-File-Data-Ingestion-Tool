#  Bidirectional ClickHouse & Flat File Data Ingestion Tool

##  Objective

A full-stack web application that enables **bidirectional data ingestion** between a **ClickHouse database** and **Flat Files (CSV)**. Users can select a source (ClickHouse or CSV), choose columns to ingest, authenticate using a **JWT token**, and view the number of processed records upon completion.

---

##  Features

-  **Bidirectional data flow:**
  - ClickHouse → Flat File (CSV)
  - Flat File → ClickHouse

-  **ClickHouse JWT Authentication**
-  **Column-wise selection** from schema
-  **Data Preview** (first 100 records)
-  **Multi-table JOIN** from ClickHouse (Bonus)
-  **Record Count Reporting**
-  **Progress Status and Error Handling**
-  Simple and intuitive UI built in React

---

##  Tech Stack

| Layer       | Technology       |
|-------------|------------------|
| Backend     | Java, Spring Boot |
| Frontend    | React.js         |
| Database    | ClickHouse       |
| File Format | CSV (Flat File)  |
| Auth        | JWT Token        |


##  Setup Instructions

###  Prerequisites

- Java 17+
- Node.js 16+
- Maven
- Docker (for running ClickHouse locally)
- Git

---

##  Backend Setup (Spring Boot)

    cd backend

# Build the project
    mvn clean install

# Run the application
    mvn spring-boot:run
    
## Frontend Setup
    cd frontend

# Install dependencies
    npm install

# Run the app
    npm start

##Author 
##Pavan Maske
