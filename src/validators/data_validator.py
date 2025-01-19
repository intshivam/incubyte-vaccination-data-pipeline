import logging
import os
import pandas as pd
import numpy as np
from datetime import datetime

from src.utils.constants import ColumnMappings
from src.utils.date_parser import DateParser


LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f'data_validator_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


invalid_records_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'invalid_records')
os.makedirs(invalid_records_dir, exist_ok=True)

class DataValidator:
    """
    Validator class for handling data validation for hospital customer records
    """
    
    EXPECTED_HEADER = '|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Country|DOB|Is_Active'
    
    @classmethod
    def validate_header(cls, header):
        """
        Validate the header record layout
        
        Args:
            header: Header string to validate
        
        Returns:
            bool: True if header is valid, False otherwise
        """
        if header != cls.EXPECTED_HEADER:
            logger.warning(f"Header does not match expected format. \nExpected: {cls.EXPECTED_HEADER}\nReceived: {header}")
            return False
        return True
    
    @classmethod
    def validate_columns(cls, df, filename=None, strict=False):
        """
        Validate presence of mandatory and optional columns
        
        Args:
            df: DataFrame to validate
            filename: Source filename (used to extract country if not in data)
            strict: If True, raise an error for missing mandatory columns
        
        Returns:
            DataFrame: Mapped and validated DataFrame
        """
        
        mapped_df = pd.DataFrame()
        processed_targets = set()
        
        for source_col in df.columns:
            if source_col in ColumnMappings.COLUMN_MAP:
                target_col = ColumnMappings.COLUMN_MAP[source_col]
                
                
                if target_col not in processed_targets:
                    
                    source_cols = [col for col, target in ColumnMappings.COLUMN_MAP.items() 
                                 if target == target_col and col in df.columns]
                    
                    
                    if len(source_cols) > 1:
                        
                        mapped_df[target_col] = df[source_cols].bfill(axis=1).iloc[:, 0]
                    else:
                        mapped_df[target_col] = df[source_col]
                    
                    processed_targets.add(target_col)
        
        
        if 'Country' not in mapped_df.columns and filename:
            
            country_code = filename[:3].upper()
            mapped_df['Country'] = country_code
            logger.info(f"Extracted country code '{country_code}' from filename")
        
        
        missing_mandatory = [col for col in ColumnMappings.MANDATORY_COLUMNS if col not in mapped_df.columns]
        if missing_mandatory:
            logger.warning(f"Missing mandatory columns: {missing_mandatory}")
            if strict:
                raise ValueError(f"Missing mandatory columns: {missing_mandatory}")
        
        
        missing_optional = [col for col in ColumnMappings.OPTIONAL_COLUMNS if col not in mapped_df.columns]
        if missing_optional:
            logger.warning(f"Missing optional columns: {missing_optional}")
        
        logger.info(f"Final columns after mapping: {mapped_df.columns.tolist()}")
        return mapped_df
    
    @classmethod
    def validate_column_types(cls, df):
        """
        Validate and clean data types of columns
        
        Args:
            df: DataFrame to validate and clean
        
        Returns:
            DataFrame: Cleaned and type-converted DataFrame
            DataFrame: Invalid records with reasons for invalidity
        """
        
        cleaned_df = df.copy()
        
        
        invalid_records = pd.DataFrame()
        
        
        string_columns = [
            'Customer_Name', 'Customer_Id', 'Vaccination_Id', 
            'Dr_Name', 'State', 'Country', 'Post_Code', 'Is_Active'
        ]
        
        
        mandatory_date_columns = ['Open_Date']
        
        
        optional_date_columns = ['Last_Consulted_Date', 'DOB']
        
        
        for col in string_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].astype(str)
        
        
        def validate_date_with_reason(x):
            try:
                DateParser.parse_date(str(x))
                return pd.NA  
            except ValueError as e:
                return str(e)
        
        
        for col in mandatory_date_columns:
            if col in cleaned_df.columns:
                
                error_messages = cleaned_df[col].apply(validate_date_with_reason)
                
                
                invalid_mask = ~error_messages.isna()
                if invalid_mask.any():
                    invalid_df = cleaned_df.loc[invalid_mask].copy()
                    invalid_df['Validation_Error'] = error_messages[invalid_mask]
                    invalid_df['Invalid_Field'] = col
                    
                    
                    invalid_records = pd.concat([invalid_records, invalid_df], ignore_index=True)
                    
                    logger.warning(f"Found {len(invalid_df)} records with invalid {col}:")
                    for _, row in invalid_df.iterrows():
                        logger.warning(f"  - Record ID {row['Customer_Id']}: {row['Validation_Error']}")
                
                
                cleaned_df.loc[invalid_mask, col] = np.nan
        
        
        for col in optional_date_columns:
            if col in cleaned_df.columns:
                
                error_messages = cleaned_df[col].apply(validate_date_with_reason)
                
                
                invalid_mask = ~error_messages.isna()
                if invalid_mask.any():
                    logger.info(f"Found {invalid_mask.sum()} records with invalid {col} (optional field):")
                    for idx in invalid_mask[invalid_mask].index:
                        logger.info(f"  - Record ID {cleaned_df.loc[idx, 'Customer_Id']}: {error_messages[idx]}")
                
                
                cleaned_df.loc[invalid_mask, col] = np.nan
        
        return cleaned_df, invalid_records
    
    @classmethod
    def save_invalid_records(cls, invalid_records):
        """
        Save invalid records to a CSV file with validation error details
        
        Args:
            invalid_records: DataFrame of invalid records with validation errors
        """
        if invalid_records is not None and not invalid_records.empty:
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"invalid_records_{timestamp}.csv"
            filepath = os.path.join(invalid_records_dir, filename)
            
            
            invalid_records.to_csv(filepath, index=False)
            logger.info(f"Saved {len(invalid_records)} invalid records to {filepath}")
            
            
            logger.info("Invalid records summary:")
            for _, row in invalid_records.iterrows():
                logger.info(f"  - Record ID {row['Customer_Id']} ({row['Customer_Name']}): "
                          f"Invalid {row['Invalid_Field']} - {row['Validation_Error']}")
    
    @classmethod
    def validate_data(cls, df, filename=None, strict=False):
        """
        Comprehensive data validation method with additional logging
        """
        try:
            logger.info("Starting validate_data")
            
            
            if df.iloc[0].str.startswith('|H|').any():
                header = df.iloc[0].str.extract('(\|H\|.*)')[0].iloc[0]
                df = df[~df.iloc[:, 0].str.startswith('|')]  
                cls.validate_header(header)
            
            
            df = cls.validate_columns(df, filename=filename, strict=strict)
            logger.info(f"After column validation shape: {df.shape}")
            
            
            cleaned_df, invalid_records = cls.validate_column_types(df)
            logger.info(f"After type validation shape: {cleaned_df.shape}")
            
            
            cls.save_invalid_records(invalid_records)
            
            logger.info("Data validation completed successfully")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error in validate_data: {str(e)}")
            logger.error(f"DataFrame info: {df.info()}")
            raise
    
    @classmethod
    def get_valid_records(cls, df):
        """
        Filter and return only valid records with enhanced debugging
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with only valid records
        """
        try:
            
            valid_df = df.copy().reset_index(drop=True)
            
            
            mask = pd.Series(True, index=valid_df.index)
            
            
            mandatory_date_columns = [col for col in ['Open_Date'] if col in valid_df.columns]
            for col in mandatory_date_columns:
                mask &= valid_df[col].notna()
            
            
            mandatory_non_date_columns = [col for col in ColumnMappings.MANDATORY_COLUMNS if col != 'Open_Date']
            for col in mandatory_non_date_columns:
                if col in valid_df.columns:
                    mask &= valid_df[col].notna() & (valid_df[col].astype(str) != '')
            
            
            valid_df = valid_df[mask]
            valid_df = ColumnMappings.map_to_snowflake_columns(valid_df)
            
            logger.info(f"Filtered valid records. Total: {len(valid_df)}")
            return valid_df
            
        except Exception as e:
            logger.error(f"Error in get_valid_records: {str(e)}")
            logger.error(f"DataFrame info: {df.info()}")
            raise