import os
import logging
from datetime import datetime
import cx_Oracle
import json
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OracleCDC:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        service_name: str,
        table_name: str,
        hwm_column: str,
        hwm_store_path: str = "hwm_store.json"
    ):
        """
        Initialize Oracle CDC processor
        
        Args:
            username: Oracle database username
            password: Oracle database password
            host: Database host
            port: Database port
            service_name: Oracle service name
            table_name: Source table to track changes
            hwm_column: Column to use as high water mark (typically a timestamp or sequential ID)
            hwm_store_path: Path to store the high water mark values
        """
        self.connection_string = f"{username}/{password}@{host}:{port}/{service_name}"
        self.table_name = table_name
        self.hwm_column = hwm_column
        self.hwm_store_path = hwm_store_path
        self.connection = None
        
    def connect(self) -> None:
        """Establish connection to Oracle database"""
        try:
            self.connection = cx_Oracle.connect(self.connection_string)
            logger.info("Successfully connected to Oracle database")
        except cx_Oracle.Error as error:
            logger.error(f"Error connecting to Oracle: {error}")
            raise
            
    def disconnect(self) -> None:
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Oracle database")
            
    def _load_hwm(self) -> Optional[str]:
        """Load the last high water mark value"""
        try:
            if os.path.exists(self.hwm_store_path):
                with open(self.hwm_store_path, 'r') as f:
                    hwm_store = json.load(f)
                    return hwm_store.get(self.table_name)
            return None
        except Exception as e:
            logger.error(f"Error loading high water mark: {e}")
            return None
            
    def _save_hwm(self, hwm_value: str) -> None:
        """Save the new high water mark value"""
        try:
            hwm_store = {}
            if os.path.exists(self.hwm_store_path):
                with open(self.hwm_store_path, 'r') as f:
                    hwm_store = json.load(f)
            
            hwm_store[self.table_name] = hwm_value
            
            with open(self.hwm_store_path, 'w') as f:
                json.dump(hwm_store, f)
            logger.info(f"Saved new high water mark: {hwm_value}")
        except Exception as e:
            logger.error(f"Error saving high water mark: {e}")
            
    def get_changes(self) -> List[Dict]:
        """
        Fetch changes since last high water mark
        Returns list of changed records
        """
        if not self.connection:
            self.connect()
            
        try:
            current_hwm = self._load_hwm()
            
            # Build query based on whether we have a previous high water mark
            if current_hwm:
                query = f"""
                    SELECT * FROM {self.table_name}
                    WHERE {self.hwm_column} > :hwm
                    ORDER BY {self.hwm_column} ASC
                """
                cursor = self.connection.cursor()
                cursor.execute(query, hwm=current_hwm)
            else:
                # First run - fetch all records
                query = f"""
                    SELECT * FROM {self.table_name}
                    ORDER BY {self.hwm_column} ASC
                """
                cursor = self.connection.cursor()
                cursor.execute(query)
                
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            # Fetch all changes
            changes = []
            new_hwm = current_hwm
            
            for row in cursor:
                record = dict(zip(columns, row))
                changes.append(record)
                # Keep track of highest water mark
                new_hwm = str(record[self.hwm_column])
                
            if new_hwm and new_hwm != current_hwm:
                self._save_hwm(new_hwm)
                
            logger.info(f"Fetched {len(changes)} changes")
            return changes
            
        except cx_Oracle.Error as error:
            logger.error(f"Error fetching changes: {error}")
            raise
            
def main():
    # Example usage
    cdc = OracleCDC(
        username="your_username",
        password="your_password",
        host="localhost",
        port=1521,
        service_name="your_service",
        table_name="your_table",
        hwm_column="last_updated_timestamp"
    )
    
    try:
        # Connect to database
        cdc.connect()
        
        # Get changes since last run
        changes = cdc.get_changes()
        
        # Process the changes (implement your logic here)
        for change in changes:
            print(f"Processing change: {change}")
            
    finally:
        # Always disconnect
        cdc.disconnect()

if __name__ == "__main__":
    main() 