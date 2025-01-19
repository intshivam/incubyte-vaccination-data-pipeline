import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

class DateParser:
    """
    Utility class for parsing dates in various formats
    """
    
    @classmethod
    def parse_date(cls, date_str: str) -> datetime:
        """
        Parse date from various possible formats
        
        Args:
            date_str (str): Date string to parse
        
        Returns:
            datetime: Parsed datetime object
        
        Raises:
            ValueError: If date cannot be parsed, with detailed error message
        """
        
        if date_str is None or (isinstance(date_str, str) and not date_str.strip()):
            raise ValueError("Empty date string")
        
        
        date_str = str(date_str).strip()
        try:
            
            float_val = float(date_str)
            date_str = str(int(float_val))
        except (ValueError, TypeError):
            pass
        
        
        
        digits_only = re.sub(r'[^\d]', '', date_str)
        
        if len(digits_only) >= 6:  
            try:
                
                if len(digits_only) == 7:  
                    month = int(digits_only[0])
                    day = int(digits_only[1:3])
                    year = int(digits_only[3:])
                else:  
                    month = int(digits_only[:2])
                    day = int(digits_only[2:4])
                    year = int(digits_only[4:])
                
                
                if year < 100:
                    year += 2000
                
                
                if month < 1 or month > 12:
                    raise ValueError(f"Invalid month: {month} (must be between 1 and 12)")
                
                if day < 1:
                    raise ValueError(f"Invalid day: {day} (must be greater than 0)")
                
                if year < 1900 or year > 2100:
                    raise ValueError(f"Invalid year: {year} (must be between 1900 and 2100)")
                
                
                days_in_month = [31, 29 if year % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                if day > days_in_month[month - 1]:
                    raise ValueError(f"Invalid day: {day} (maximum {days_in_month[month - 1]} days in month {month})")
                
                return datetime(year, month, day)
            except (ValueError, TypeError, IndexError) as e:
                if isinstance(e, ValueError) and str(e).startswith("Invalid"):
                    raise  
                
        
        
        
        date_str = re.sub(r'[^\d/-]', '', date_str)
        
        
        date_formats = [
            
            '%m/%d/%Y',   
            '%Y/%m/%d',   
            '%d/%m/%Y',   
            
            
            '%Y-%m-%d',   
            '%m-%d-%Y',   
            '%d-%m-%Y',   
            
            
            '%Y%m%d',     
            '%m%d%Y',     
            '%d%m%Y',     
        ]
        
        
        last_error = None
        for fmt in date_formats:
            try:
                
                parsed_date = datetime.strptime(date_str, fmt)
                
                
                if parsed_date.year < 1900 or parsed_date.year > 2100:
                    last_error = ValueError(f"Invalid year: {parsed_date.year} (must be between 1900 and 2100)")
                    continue
                
                
                if parsed_date.month < 1 or parsed_date.month > 12:
                    last_error = ValueError(f"Invalid month: {parsed_date.month} (must be between 1 and 12)")
                    continue
                
                
                days_in_month = [31, 29 if parsed_date.year % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                if parsed_date.day < 1 or parsed_date.day > days_in_month[parsed_date.month - 1]:
                    last_error = ValueError(f"Invalid day: {parsed_date.day} (maximum {days_in_month[parsed_date.month - 1]} days in month {parsed_date.month})")
                    continue
                
                return parsed_date
            except ValueError as e:
                last_error = e
                continue
        
        
        if last_error:
            raise ValueError(f"Unable to parse date '{date_str}': {str(last_error)}")
        else:
            raise ValueError(f"Unable to parse date '{date_str}': format not recognized")
    
    @classmethod
    def validate_date(cls, date_str: str) -> bool:
        """
        Validate if a date string can be parsed
        
        Args:
            date_str (str): Date string to validate
        
        Returns:
            bool: True if date can be parsed, False otherwise
        """
        try:
            cls.parse_date(date_str)
            return True
        except ValueError:
            return False
