class ColumnMappings:
    """
    Mapping of column names across different input formats to standardized names
    """
    # Mapping of source column names to standardized names
    COLUMN_MAP = {
        # USA file columns
        'ID': 'Customer_Id',
        'Name': 'Customer_Name',
        'VaccinationType': 'Vaccination_Id',
        'VaccinationDate': 'Open_Date',
        
        # AUS file columns
        'Unique ID': 'Customer_Id',
        'Patient Name': 'Customer_Name',
        'Vaccine Type': 'Vaccination_Id',
        'Date of Birth': 'DOB',
        'Date of Vaccination': 'Open_Date',
        
        # IND file columns
        'DOB': 'DOB',
        'VaccinationType': 'Vaccination_Id',
        'VaccinationDate': 'Open_Date',
        
        # Additional columns
        'Doctor Name': 'Dr_Name',
        'Doctor': 'Dr_Name',
        'State/Province': 'State',
        'State': 'State',
        'Country Name': 'Country',
        'Country': 'Country',
        'Consultation Date': 'Last_Consulted_Date',
        'Last Consulted Date': 'Last_Consulted_Date',
        'Postal Code': 'Post_Code',
        'Post Code': 'Post_Code'
    }

    # Mandatory columns for validation
    MANDATORY_COLUMNS = [
        'Customer_Name', 
        'Customer_Id', 
        'Open_Date'
    ]

    # Optional columns for validation
    OPTIONAL_COLUMNS = [
        'Last_Consulted_Date', 
        'Vaccination_Id', 
        'Dr_Name', 
        'State', 
        'Country', 
        'Post_Code', 
        'DOB'
    ]

    # Intermediate Snowflake table column mapping
    SNOWFLAKE_COLUMN_MAP = {
        'Customer_Name': 'Name',
        'Customer_Id': 'Cust_I',
        'Open_Date': 'Open_Dt',
        'Last_Consulted_Date': 'Consul_Dt',
        'Vaccination_Id': 'VAC_ID',
        'Dr_Name': 'DR_Name',
        'State': 'State',
        'Country': 'Country',
        'DOB': 'DOB',
        'Is_Active': 'FLAG'
    }

    @classmethod
    def map_columns(cls, df):
        """
        Map source column names to standardized names
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with mapped column names
        """
        # Create a mapping of current column names to standardized names
        column_mapping = {col: cls.COLUMN_MAP.get(col, col) for col in df.columns}
        
        # Rename columns
        return df.rename(columns=column_mapping)

    @classmethod
    def map_to_snowflake_columns(cls, df):
        """
        Map standardized columns to Snowflake intermediate table columns
        
        Args:
            df: Input DataFrame with standardized column names
        
        Returns:
            DataFrame with Snowflake column names
        """
        # Create a mapping of current column names to Snowflake names
        snowflake_mapping = {col: cls.SNOWFLAKE_COLUMN_MAP.get(col, col) for col in df.columns}
        
        # Rename columns
        return df.rename(columns=snowflake_mapping)
