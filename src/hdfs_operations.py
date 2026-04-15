"""
E-Learning Progress Tracker - HDFS Operations
Handles all HDFS interactions: upload, download, directory management
"""

import subprocess
import os
from pathlib import Path

class HDFSManager:
    def __init__(self, hdfs_base_path='/user/elearning'):
        self.hdfs_base_path = hdfs_base_path
        self.hdfs_raw_path = f'{hdfs_base_path}/raw'
        self.hdfs_processed_path = f'{hdfs_base_path}/processed'
        self.hdfs_results_path = f'{hdfs_base_path}/results'
        
    def run_hdfs_command(self, command):
        """Execute HDFS command and return output"""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {command}")
            print(f"Error: {e.stderr}")
            return None
    
    def create_hdfs_directories(self):
        """Create HDFS directory structure"""
        print("\n" + "="*60)
        print("Creating HDFS Directory Structure")
        print("="*60 + "\n")
        
        directories = [
            self.hdfs_base_path,
            self.hdfs_raw_path,
            self.hdfs_processed_path,
            self.hdfs_results_path,
            f'{self.hdfs_results_path}/analytics',
            f'{self.hdfs_results_path}/reports'
        ]
        
        for directory in directories:
            print(f"Creating: {directory}")
            self.run_hdfs_command(f'hdfs dfs -mkdir -p {directory}')
        
        print("\nHDFS directories created successfully!")
        self.list_hdfs_structure()
    
    def upload_data_to_hdfs(self, local_data_dir='data/raw'):
        """Upload all CSV files from local directory to HDFS"""
        print("\n" + "="*60)
        print("Uploading Data to HDFS")
        print("="*60 + "\n")
        
        if not os.path.exists(local_data_dir):
            print(f"Local directory not found: {local_data_dir}")
            return
        
        csv_files = list(Path(local_data_dir).glob('*.csv'))
        
        if not csv_files:
            print(f"No CSV files found in {local_data_dir}")
            return
        
        for csv_file in csv_files:
            print(f"Uploading: {csv_file.name}")
            command = f'hdfs dfs -put -f {csv_file} {self.hdfs_raw_path}/'
            self.run_hdfs_command(command)
        
        print(f"\nUploaded {len(csv_files)} files to HDFS")
        self.list_files(self.hdfs_raw_path)
    
    def list_hdfs_structure(self):
        """Display HDFS directory structure"""
        print("\nHDFS Directory Structure:")
        print("-" * 60)
        output = self.run_hdfs_command(f'hdfs dfs -ls -R {self.hdfs_base_path}')
        if output:
            print(output)
    
    def list_files(self, hdfs_path):
        """List files in specific HDFS directory"""
        print(f"\nFiles in {hdfs_path}:")
        print("-" * 60)
        output = self.run_hdfs_command(f'hdfs dfs -ls {hdfs_path}')
        if output:
            print(output)
    
    def download_from_hdfs(self, hdfs_path, local_path):
        """Download file from HDFS to local"""
        print(f"\nDownloading {hdfs_path} to {local_path}")
        command = f'hdfs dfs -get -f {hdfs_path} {local_path}'
        result = self.run_hdfs_command(command)
        if result is not None:
            print("Download successful")
        return result
    
    def delete_hdfs_path(self, hdfs_path):
        """Delete file or directory from HDFS"""
        print(f"\nDeleting {hdfs_path} from HDFS")
        command = f'hdfs dfs -rm -r {hdfs_path}'
        result = self.run_hdfs_command(command)
        if result is not None:
            print("Deletion successful")
        return result
    
    def get_file_info(self, hdfs_path):
        """Get file information from HDFS"""
        print(f"\nFile Info: {hdfs_path}")
        print("-" * 60)
        
        # File size
        du_output = self.run_hdfs_command(f'hdfs dfs -du -h {hdfs_path}')
        if du_output:
            print(f"Size: {du_output.strip()}")
        
        # File count
        count_output = self.run_hdfs_command(f'hdfs dfs -count {hdfs_path}')
        if count_output:
            print(f"Count: {count_output.strip()}")
    
    def cat_file(self, hdfs_path, num_lines=10):
        """Display file content from HDFS"""
        print(f"\nFirst {num_lines} lines of {hdfs_path}:")
        print("-" * 60)
        command = f'hdfs dfs -cat {hdfs_path} | head -{num_lines}'
        output = self.run_hdfs_command(command)
        if output:
            print(output)
    
    def check_hdfs_health(self):
        """Check HDFS health and status"""
        print("\n" + "="*60)
        print("HDFS Health Check")
        print("="*60 + "\n")
        
        # HDFS report
        print("HDFS Report:")
        print("-" * 60)
        report = self.run_hdfs_command('hdfs dfsadmin -report')
        if report:
            # Extract key information
            for line in report.split('\n')[:20]:  # Show first 20 lines
                print(line)
        
        # Disk usage
        print("\nDisk Usage:")
        print("-" * 60)
        df_output = self.run_hdfs_command('hdfs dfs -df -h')
        if df_output:
            print(df_output)
    
    def setup_complete_environment(self, local_data_dir='data/raw'):
        """Complete HDFS setup: create dirs and upload data"""
        print("\n" + "="*60)
        print("COMPLETE HDFS ENVIRONMENT SETUP")
        print("="*60)
        
        # Step 1: Create directories
        self.create_hdfs_directories()
        
        # Step 2: Upload data
        self.upload_data_to_hdfs(local_data_dir)
        
        # Step 3: Verify
        self.check_hdfs_health()
        
        print("\n" + "="*60)
        print("HDFS Environment Setup Complete!")
        print("="*60 + "\n")


def test_hdfs_connection():
    """Test if HDFS is running and accessible"""
    print("\nTesting HDFS Connection...")
    try:
        result = subprocess.run(
            'hdfs dfs -ls /',
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        print("HDFS is running and accessible!")
        return True
    except subprocess.CalledProcessError:
        print("HDFS is not running or not accessible")
        print("\nPlease start HDFS with:")
        print("  start-dfs.sh")
        print("  start-yarn.sh")
        return False


if __name__ == "__main__":
    # Test connection first
    if test_hdfs_connection():
        # Initialize HDFS Manager
        hdfs_manager = HDFSManager()
        
        # Setup complete environment
        hdfs_manager.setup_complete_environment(local_data_dir='data/raw')
        
        # Display some sample data
        print("\nSample Data from HDFS:")
        hdfs_manager.cat_file(f'{hdfs_manager.hdfs_raw_path}/students.csv', num_lines=5)