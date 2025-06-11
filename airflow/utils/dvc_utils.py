import subprocess
import os
import logging
from pathlib import Path

class DVCManager:
    """DVC utilities for data versioning in container environment"""
    
    def __init__(self, project_path: str = "/opt/airflow/magnitudr"):
        self.project_path = Path(project_path)
        self.credential_path = "/opt/airflow/my-dvc-credential.json"
    
    def initialize_dvc(self):
        """Initialize DVC if not already done"""
        try:
            if not (self.project_path / ".dvc").exists():
                subprocess.run(["dvc", "init"], cwd=self.project_path, check=True)
                logging.info("✅ DVC initialized")
            
            # Configure Google Drive remote
            subprocess.run([
                "dvc", "remote", "add", "-d", "gdrive", 
                "gdrive://1your_drive_folder_id"  # Replace with actual folder ID
            ], cwd=self.project_path, check=False)
            
            # Set authentication
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credential_path
            
        except Exception as e:
            logging.warning(f"⚠️ DVC initialization failed: {e}")
    
    def add_and_push_data(self, data_path: str):
        """Add data to DVC and push to remote"""
        try:
            # Ensure DVC is initialized
            self.initialize_dvc()
            
            # Add data to DVC
            subprocess.run(
                ["dvc", "add", data_path], 
                cwd=self.project_path, 
                check=True
            )
            logging.info(f"✅ DVC add: {data_path}")
            
            # Push to remote storage
            subprocess.run(
                ["dvc", "push"], 
                cwd=self.project_path, 
                check=True
            )
            logging.info(f"✅ DVC push: {data_path}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ DVC operation failed: {e}")
            return False
        except Exception as e:
            logging.warning(f"⚠️ DVC operation warning: {e}")
            return False
