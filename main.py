import os
import logging
import pandas as pd
from datetime import datetime
from src.utils.constants import ColumnMappings


from src.validators.data_validator import DataValidator
from src.utils.snowflake_connector import SnowflakeConnector
from src.utils.view_generator import generate_country_views


LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)


log_filename = os.path.join(LOG_DIR, f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),  
        logging.StreamHandler()  
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
    
    dfs = []
    
    
    for filename in os.listdir(data_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_directory, filename)
            logger.info(f"Loading data from {filename}")
            
            
            df = pd.read_csv(file_path)
            
            
            validated_df = DataValidator.validate_data(df, filename=filename)
            
            
            dfs.append(validated_df)
    
    
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
            
            query = "SELECT DISTINCT COUNTRY FROM INTERMEDIATE_VACCINATION_RECORDS"
            
            
            cursor = sf.connection.cursor()
            cursor.execute(query)
            
            
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
        
        countries = fetch_countries_from_snowflake()
        
        
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
        
        with SnowflakeConnector() as sf:
            
            view_dir = 'scripts/dml/generated'
            view_files = [f for f in os.listdir(view_dir) if f.endswith('.sql')]
            
            
            view_files.sort()
            
            
            for view_file in view_files:
                full_path = os.path.join(view_dir, view_file)
                
                
                with open(full_path, 'r') as f:
                    view_sql = f.read()
                
                
                cursor = sf.connection.cursor()
                cursor.execute(view_sql)
                
                logger.info(f"Executed view from {view_file}")
            
            logger.info("All country-specific views executed successfully")
    
    except Exception as e:
        logger.error(f"Error executing country views: {e}")
        raise

def main():
    try:
        
        source_data = load_source_data()
        
        
        valid_records = DataValidator.get_valid_records(source_data)
        
        
        sf_connector = SnowflakeConnector()
        write_success = sf_connector.write_to_snowflake(valid_records)
        
        if write_success:
            
            generate_country_specific_views()
            
            
            execute_country_views()
            logger.info("ETL pipeline completed successfully")
        else:
            logger.error("ETL pipeline failed during Snowflake write")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()