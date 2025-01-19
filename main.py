import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.constants import ColumnMappings

# Import project modules
from src.validators.data_validator import DataValidator
from src.utils.snowflake_connector import SnowflakeConnector
from src.utils.view_generator import generate_country_views

# Configure logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Generate unique log filename with timestamp
log_filename = os.path.join(LOG_DIR, f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Configure logging with unique file for each run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),  # Log to unique file
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

def load_source_data(data_directory='data'):
    """
    Load and validate source data files from the data directory
    
    Args:
        data_directory: Directory containing source data files
    
    Returns:
        DataFrame: Combined and validated DataFrame
    """
    # Initialize empty list to store DataFrames
    dfs = []
    
    # Load each CSV file in the data directory
    for filename in os.listdir(data_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_directory, filename)
            logger.info(f"Loading data from {filename}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Validate data
            validated_df = DataValidator.validate_data(df, filename=filename)
            
            # Append to list
            dfs.append(validated_df)
    
    # Combine all DataFrames
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        raise ValueError("No data files found in data directory")

def fetch_countries_from_snowflake():
    """
    Fetch unique countries from the intermediate Snowflake table
    
    Returns:
        list: Unique countries
    """
    try:
        with SnowflakeConnector() as sf:
            # Query to fetch unique countries
            query = "SELECT DISTINCT COUNTRY FROM INTERMEDIATE_VACCINATION_RECORDS"
            
            # Execute query
            cursor = sf.connection.cursor()
            cursor.execute(query)
            
            # Fetch all countries
            countries = [row[0] for row in cursor.fetchall() if row[0]]
            
            logger.info(f"Fetched countries from Snowflake: {countries}")
            return countries
    
    except Exception as e:
        logger.error(f"Error fetching countries from Snowflake: {e}")
        raise

def generate_country_specific_views():
    """
    Generate views for each country in the intermediate table
    """
    try:
        # Fetch countries from Snowflake
        countries = fetch_countries_from_snowflake()
        
        # Generate views for these countries
        generate_country_views(countries)
        
        logger.info("Country-specific views generated successfully")
    
    except Exception as e:
        logger.error(f"Error generating country views: {e}")
        raise

def execute_country_views():
    """
    Execute generated country-specific views in Snowflake
    """
    try:
        # Initialize Snowflake connector
        with SnowflakeConnector() as sf:
            # Get list of view files
            view_dir = 'scripts/dml/generated'
            view_files = [f for f in os.listdir(view_dir) if f.endswith('.sql')]
            
            # Sort views to ensure consistent order
            view_files.sort()
            
            # Execute each view
            for view_file in view_files:
                full_path = os.path.join(view_dir, view_file)
                
                # Read view SQL
                with open(full_path, 'r') as f:
                    view_sql = f.read()
                
                # Execute view creation
                cursor = sf.connection.cursor()
                cursor.execute(view_sql)
                
                logger.info(f"Executed view from {view_file}")
            
            logger.info("All country-specific views executed successfully")
    
    except Exception as e:
        logger.error(f"Error executing country views: {e}")
        raise

def main():
    try:
        # Step 1: Load source data
        source_data = load_source_data()
        
        # Step 2: Get valid records
        valid_records = DataValidator.get_valid_records(source_data)
        
        # Step 3: Write valid records to Snowflake
        sf_connector = SnowflakeConnector()
        write_success = sf_connector.write_to_snowflake(valid_records)
        
        if write_success:
            # Step 4: Generate country-specific views
            generate_country_specific_views()
            
            # Step 5: Execute generated views
            execute_country_views()
            logger.info("ETL pipeline completed successfully")
        else:
            logger.error("ETL pipeline failed during Snowflake write")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()