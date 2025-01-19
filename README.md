# Vaccination Data Processing Pipeline

## Project Overview
This project implements a robust data engineering solution for processing vaccination records, designed to handle large-scale healthcare data efficiently and securely.



## Features
- 🔍 Data Validation
- 🔄 ETL Processing
- ❄️ Snowflake Integration
- 📊 Comprehensive Error Handling
- 📝 Dynamic Logging
- 🌍 Country-Specific Data Views

## Prerequisites
- Python 3.13+
- Snowflake Account
- Virtual Environment Support

## Database Setup
Before running the ETL pipeline, execute the following DDL script in your Snowflake database:
```bash
snowsql -f scripts/ddl/create_intermediate_table.sql
```
**Note**: Ensure you are connected to the correct Snowflake account and have the necessary permissions to create tables.

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/intshivam/incubyte-vaccination-data-pipeline.git
cd incubyte-vaccination-data-pipeline
```

### 2. Set Up Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a `.env` file with the following Snowflake credentials:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Project Structure
```
incubyte_assessment/
│
├── src/
│   ├── utils/
│   │   ├── snowflake_connector.py     # Enhanced Snowflake connection
│   │   ├── constants.py               # Column mappings
│   │   └── view_generator.py          # View generation logic
│   │
│   ├── validators/
│   │   └── data_validator.py          # Data validation
│   │
│   └── transformers/
│       └── data_transformer.py
│
├── scripts/
│   └── dml/
│       └── generated/                 # Dynamically generated SQL views
│
├── logs/                              # Unique log files for each run
│
├── data/
│   └── invalid_records/               # Invalid record tracking
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

## Design Rationale

### Views vs Tables for Country-Specific Data
We chose to use SQL views instead of additional tables for several key reasons:
- **Storage Optimization**: All raw data is stored in a single intermediate table, reducing redundant data storage.
- **Snowflake Efficiency**: Snowflake is highly optimized for view performance, making views a lightweight and efficient solution.
- **Dynamic Derived Columns**: Views allow for dynamic calculations like:
  - Age computation
  - Identifying records with last consultation date > 30 days
- **Computational Efficiency**: Avoiding table refreshes for every record, which would be computationally expensive.

### Scalability Considerations
While processing billions of records ideally would use distributed architectures like PySpark, our current implementation focuses on optimization within a single-machine Python environment:
- **Batch Processing**: Utilized `chunk_size` parameter in `write_to_pandas` to enable efficient batch writing to Snowflake.
- **Memory Management**: Implemented chunk-based processing to handle large datasets without overwhelming system resources.
- **Future Scalability**: Design allows for easy migration to distributed processing frameworks like PySpark when needed.

### Performance Optimization Strategies
- Minimal data transformation
- Efficient column mapping
- Batch processing
- Leveraging Snowflake's view capabilities

## Running the Pipeline
```bash
python3 main.py
```

## Logging
- Unique log files are generated for each ETL pipeline run
- Logs are stored in the `logs/` directory
- Includes detailed information about data validation, Snowflake operations, and view generation

## Key Components
- **SnowflakeConnector**: Manages Snowflake database connections
- **DataValidator**: Validates and cleanses input data
- **ViewGenerator**: Creates country-specific SQL views

## Error Handling
- Comprehensive logging and error tracking
- Invalid records are saved in `data/invalid_records/`
- Detailed error messages for debugging

## Security Considerations
- Sensitive credentials managed via environment variables
- Dynamic log files prevent log file conflicts
- Gitignore prevents sensitive files from being tracked