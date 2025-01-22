import os
import sys

def generate_country_views(countries, output_dir='scripts/dml/generated'):
    """
    Generate SQL view queries for specified countries
    
    Args:
        countries (list): List of countries to generate views for
        output_dir (str): Directory to store generated view SQL files
    """
    
    os.makedirs(output_dir, exist_ok=True)
    
    for country in countries:
        view_name = f"VIEW_{country.replace(' ', '_').upper()}"
        view_query = f"""
CREATE OR REPLACE VIEW VACCINATION_DATA.{view_name} AS
WITH RankedCustomers AS (
    SELECT 
        CUST_I,
        NAME,
        OPEN_DT,
        CONSUL_DT,
        VAC_ID,
        DR_NAME,
        STATE,
        COUNTRY,
        DOB,
        FLAG,
        
        -- Computed column: Age
        DATEDIFF(YEAR, DOB, CURRENT_DATE()) AS AGE,
        
        -- Computed column: Days Since Last Consulted > 30
        CASE 
            WHEN DATEDIFF(DAY, CONSUL_DT, CURRENT_DATE()) > 30 
            THEN TRUE 
            ELSE FALSE 
        END AS DAYS_SINCE_CONSUL_GT_30,
        
        ROW_NUMBER() OVER (
            PARTITION BY CUST_I 
            ORDER BY CONSUL_DT DESC
        ) as RowNum
    FROM VACCINATION_DATA.INTERMEDIATE_VACCINATION_RECORDS
    
)
SELECT 
    CUST_I,
    NAME,
    OPEN_DT,
    CONSUL_DT,
    VAC_ID,
    DR_NAME,
    STATE,
    COUNTRY,
    DOB,
    FLAG,
    AGE,
    DAYS_SINCE_CONSUL_GT_30
FROM RankedCustomers
WHERE RowNum = 1
AND COUNTRY = '{country}';
"""
        
        
        view_filename = os.path.join(output_dir, f"{view_name}.sql")
        with open(view_filename, 'w') as f:
            f.write(view_query)
        
        print(f"Generated view for {country}: {view_filename}")

def main():
    
    countries = ['India', 'USA', 'UK', 'Canada']
    generate_country_views(countries)

if __name__ == "__main__":
    main()
