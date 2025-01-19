import os
import logging
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime
from src.utils.constants import ColumnMappings

# Create logs directory if it doesn't exist
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'snowflake_connector_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """
    Utility class for managing Snowflake connections and data operations
    """
    
    def __init__(self, 
                 account=None, 
                 username=None, 
                 password=None, 
                 warehouse=None, 
                 database=None, 
                 schema=None,
                 disable_ssl_verification=False):
        """
        Initialize Snowflake connection parameters
        
        Args:
            account: Snowflake account identifier
            username: Snowflake username
            password: Snowflake password
            warehouse: Snowflake warehouse
            database: Snowflake database
            schema: Snowflake schema
            disable_ssl_verification: Disable SSL certificate verification (for testing only)
        """
        # Load environment variables if not provided
        load_dotenv()
        
        # Use environment variables or provided parameters
        # Extract account identifier from full hostname
        raw_account = account or os.getenv('SNOWFLAKE_ACCOUNT')
        if raw_account:
            # Remove .snowflakecomputing.com or similar suffixes
            self.account = raw_account.split('.')[0]
        else:
            self.account = None
        
        self.username = username or os.getenv('SNOWFLAKE_USERNAME')
        self.password = password or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = warehouse or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = database or os.getenv('SNOWFLAKE_DATABASE')
        self.schema = schema or os.getenv('SNOWFLAKE_SCHEMA')
        
        # SSL verification flag
        self.disable_ssl_verification = disable_ssl_verification
        
        # Validate required parameters
        self._validate_credentials()
        
        # Connection object
        self.connection = None
        self.cursor = None
    
    def _validate_credentials(self):
        """
        Validate that all required Snowflake connection parameters are present
        
        Raises:
            ValueError: If any required parameter is missing
        """
        required_params = [
            ('account', self.account),
            ('username', self.username),
            ('password', self.password),
            ('warehouse', self.warehouse),
            ('database', self.database),
            ('schema', self.schema)
        ]
        
        missing_params = [param for param, value in required_params if not value]
        if missing_params:
            raise ValueError(f"Missing Snowflake connection parameters: {', '.join(missing_params)}")
    
    def connect(self):
        """
        Establish a connection to Snowflake
        
        Returns:
            snowflake.connector.connection.SnowflakeConnection: Active connection
        """
        try:
            # Prepare connection parameters
            conn_params = {
                'account': self.account,
                'user': self.username,
                'password': self.password,
                'warehouse': self.warehouse,
                'database': self.database,
                'schema': self.schema
            }
            
            # Add SSL verification option if disabled
            if self.disable_ssl_verification:
                logger.warning("SSL certificate verification is DISABLED. This is NOT recommended for production!")
                conn_params['insecure_mode'] = True
            
            # Establish connection
            self.connection = snowflake.connector.connect(**conn_params)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Snowflake")
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self):
        """
        Close Snowflake connection and cursor
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {e}")
    
    def execute_query(self, query, params=None):
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
        
        Returns:
            list: Query results
        """
        try:
            # Ensure connection is active
            if not self.connection:
                self.connect()
            
            # Execute query
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def write_dataframe(self, 
                         df, 
                         table_name, 
                         database=None, 
                         schema=None, 
                         source_file=None,
                         chunk_size=50000,
                         parallel_threads=4):
        """
        Write a large pandas DataFrame to a Snowflake table using write_pandas with chunking
        
        Args:
            df: Pandas DataFrame to write
            table_name: Target table name
            database: Optional database name (uses default if not specified)
            schema: Optional schema name (uses default if not specified)
            source_file: Optional source file name to track data origin
            chunk_size: Number of rows to write in each chunk (default 50,000)
            parallel_threads: Number of parallel threads for writing (default 4)
        
        Returns:
            tuple: Total success status, total chunks, total rows inserted
        """
        try:
            if not self.connection:
                self.connect()
            
            # Use provided or default database and schema
            target_database = database or self.database
            target_schema = schema or self.schema
            
            # Add source file column if provided
            if source_file and 'SOURCE_FILE' not in df.columns:
                df['SOURCE_FILE'] = source_file
            
            # Sanitize column names for Snowflake
            df = df.copy()
            df.columns = [col.upper().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Validate chunk size
            chunk_size = max(1000, min(chunk_size, 100000))  # Constrain chunk size between 1000 and 100,000
            
            # Total tracking variables
            total_success = True
            total_nchunks = 0
            total_nrows = 0
            
            # Chunked writing with optional parallel processing
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                
                try:
                    # Write chunk to Snowflake
                    success, nchunks, nrows, _ = write_pandas(
                        conn=self.connection,
                        df=chunk,
                        table_name=table_name,
                        database=target_database,
                        schema=target_schema
                    )
                    
                    # Update tracking variables
                    total_success &= success
                    total_nchunks += nchunks
                    total_nrows += nrows
                    
                    # Log progress
                    logger.info(f"Wrote chunk {i//chunk_size + 1}: {nrows} rows")
                    
                except Exception as chunk_error:
                    logger.error(f"Error writing chunk {i//chunk_size + 1}: {chunk_error}")
                    total_success = False
            
            # Final logging
            logger.info(f"Successfully wrote {total_nrows} rows to {target_database}.{target_schema}.{table_name}")
            
            return total_success, total_nchunks, total_nrows
        
        except Exception as e:
            logger.error(f"Error writing DataFrame to Snowflake: {e}")
            raise
    
    def write_to_snowflake(self, df, table_name='INTERMEDIATE_VACCINATION_RECORDS', database='INCUBYTE', schema='VACCINATION_DATA'):
        """
        Write DataFrame to Snowflake table
        
        Args:
            df (pd.DataFrame): DataFrame to write
            table_name (str, optional): Target table name. Defaults to 'INTERMEDIATE_VACCINATION_RECORDS'.
            database (str, optional): Target database. Defaults to 'INCUBYTE'.
            schema (str, optional): Target schema. Defaults to 'VACCINATION_DATA'.
        
        Returns:
            bool: True if write was successful, False otherwise
        """
        try:
            # Ensure connection is established
            if not self.connection:
                self.connect()
            
            # Prepare DataFrame for Snowflake
            write_df = df.copy().reset_index(drop=True)
            
            # Log original columns
            logger.info(f"Original DataFrame columns: {list(write_df.columns)}")
            
            # Map to Snowflake column names
            write_df = ColumnMappings.map_to_snowflake_columns(write_df)
            
            # Remove quotes from column names
            write_df.columns = write_df.columns.str.replace('"', '')
            
            # Log mapped columns
            logger.info(f"Mapped DataFrame columns: {list(write_df.columns)}")
            
            # Convert date columns to a consistent format
            date_columns = ['Open_Dt', 'Consul_Dt', 'DOB']
            
            for col in date_columns:
                if col in write_df.columns:
                    # Convert to datetime first, then to a string in YYYY-MM-DD format
                    write_df[col] = pd.to_datetime(write_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Prepare cursor
            cursor = self.connection.cursor()
            
            # Prepare full table name
            full_table_name = f"{database}.{schema}.{table_name}"
            
            # Write DataFrame to Snowflake
            success, nchunks, nrows = self.write_dataframe(
                df=write_df, 
                table_name=table_name,
                source_file='snowflake_connector.py',
                chunk_size=10000
            )
            
            # Log results
            logger.info(f"Successfully wrote {nrows} rows to {full_table_name}")
            logger.info(f"Data write results - Success: {success}, Chunks: {nchunks}, Rows: {nrows}")
            
            return success
        
        except Exception as e:
            logger.error(f"Error writing to Snowflake: {e}")
            raise
        
        finally:
            # Close cursor and connection
            if cursor:
                cursor.close()

    def __enter__(self):
        """
        Context manager entry point
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point
        """
        self.close()

# Example usage
if __name__ == '__main__':
    # Demonstrate basic usage
    try:
        with SnowflakeConnector() as sf:
            # Example query
            results = sf.execute_query("SELECT CURRENT_WAREHOUSE()")
            print("Current Warehouse:", results[0][0])
    except Exception as e:
        print(f"An error occurred: {e}")
