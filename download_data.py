#!/usr/bin/env python3
"""
Download NYC Taxi Trip Data

This script downloads NYC Yellow Taxi trip data from the official source.
Downloads 3 months of data (Jan-Mar 2023) for the PySpark assignment.
"""

import os
import urllib.request
import sys
from pathlib import Path

# NYC TLC data URL base
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Files to download (Q1 2023)
FILES = [
    "yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet",
]

# Output directory
DATA_DIR = Path("./data")


def format_bytes(bytes_size):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def download_file(url, dest_path):
    """Download a file with progress indication"""
    print(f"\nDownloading: {url}")
    print(f"Destination: {dest_path}")
    
    def report_progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        percent = min(100, (downloaded / total_size) * 100)
        downloaded_str = format_bytes(downloaded)
        total_str = format_bytes(total_size)
        
        # Progress bar
        bar_length = 50
        filled_length = int(bar_length * downloaded // total_size)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        
        sys.stdout.write(f'\r[{bar}] {percent:.1f}% ({downloaded_str}/{total_str})')
        sys.stdout.flush()
    
    try:
        urllib.request.urlretrieve(url, dest_path, report_progress)
        print("\n✓ Download complete!")
        return True
    except Exception as e:
        print(f"\n✗ Error downloading file: {e}")
        return False


def main():
    """Main download function"""
    print("="*70)
    print("NYC Taxi Data Download Script")
    print("="*70)
    print(f"\nThis will download {len(FILES)} files (~3GB total)")
    print(f"Destination: {DATA_DIR.absolute()}")
    
    # Create data directory if it doesn't exist
    DATA_DIR.mkdir(exist_ok=True)
    print(f"\n✓ Data directory ready: {DATA_DIR}")
    
    # Check if files already exist
    existing_files = []
    for filename in FILES:
        filepath = DATA_DIR / filename
        if filepath.exists():
            existing_files.append(filename)
    
    if existing_files:
        print(f"\nWarning: {len(existing_files)} file(s) already exist:")
        for f in existing_files:
            print(f"  - {f}")
        
        response = input("\nDo you want to re-download existing files? (y/N): ")
        if response.lower() != 'y':
            print("\nSkipping existing files...")
            FILES[:] = [f for f in FILES if f not in existing_files]
    
    if not FILES:
        print("\nAll files already downloaded. Nothing to do!")
        return
    
    print(f"\nStarting download of {len(FILES)} file(s)...")
    
    # Download each file
    successful = 0
    failed = 0
    
    for i, filename in enumerate(FILES, 1):
        print(f"\n[{i}/{len(FILES)}] Processing: {filename}")
        
        url = BASE_URL + filename
        dest_path = DATA_DIR / filename
        
        if download_file(url, dest_path):
            successful += 1
            # Verify file size
            file_size = dest_path.stat().st_size
            print(f"File size: {format_bytes(file_size)}")
        else:
            failed += 1
    
    # Summary
    print("\n" + "="*70)
    print("Download Summary")
    print("="*70)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n✓ All files downloaded successfully!")
        print(f"\nData is ready in: {DATA_DIR.absolute()}")
        print("\nNext steps:")
        print("1. Update the notebook to use local data:")
        print(f"   base_path = '{DATA_DIR}/yellow_tripdata_2023-*.parquet'")
        print("2. Run the Jupyter notebook:")
        print("   jupyter notebook pyspark_assignment.ipynb")
    else:
        print("\n✗ Some downloads failed. Please check your internet connection and try again.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDownload cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        sys.exit(1)