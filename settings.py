from dataclasses import dataclass
from pathlib import Path
from loguru import logger

@dataclass
class Settings:
    server: str
    """Connection String for SQL Server"""
    database: str
    """Database name inside SQL Server"""
    data_dir: Path
    """Directory containing data to extract"""
    log_dir: Path
    """Directory for logs"""
