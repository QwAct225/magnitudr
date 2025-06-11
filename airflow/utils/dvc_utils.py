import subprocess
import os
import logging
from pathlib import Path

class DVCManager:
    """DVC utilities using existing configuration"""
    
    def __init__(self, project_path: str = "/opt/airflow/magnitudr"):
        self.project_path = Path(project_path)
        self.credential_path = self.project_path / "my-dvc-credential.json"
    
    def check_dvc_status(self):
        """Check DVC status and configuration"""
        try:
            # Check if DVC is properly configured
            result = subprocess.run(
                ["dvc", "remote", "list"], 
                cwd=self.project_path, 
                capture_output=True, 
                text=True
            )
            
            if "storage" in result.stdout:
                logging.info("✅ DVC remote 'storage' configured")
                return True
            else:
                logging.warning("⚠️ DVC remote 'storage' not found")
                return False
                
        except Exception as e:
            logging.error(f"❌ DVC status check failed: {e}")
            return False
    
    def add_and_push_data(self, data_path: str):
        """Add data to DVC and push using existing 'storage' remote"""
        try:
            # Set environment for service account
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(self.credential_path)
            
            # Ensure we're in the right directory
            os.chdir(self.project_path)
            
            # Add data to DVC (relative to project root)
            relative_path = os.path.relpath(data_path, self.project_path)
            
            subprocess.run(
                ["dvc", "add", relative_path], 
                cwd=self.project_path, 
                check=True
            )
            logging.info(f"✅ DVC add: {relative_path}")
            
            # Push to existing 'storage' remote
            subprocess.run(
                ["dvc", "push", "-r", "storage"], 
                cwd=self.project_path, 
                check=True
            )
            logging.info(f"✅ DVC push to storage: {relative_path}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ DVC operation failed: {e}")
            return False
        except Exception as e:
            logging.warning(f"⚠️ DVC operation warning: {e}")
            return False
