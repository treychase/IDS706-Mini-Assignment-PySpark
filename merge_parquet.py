#!/usr/bin/env python3
"""
Merge NYC Taxi Parquet Files

This script combines all downloaded monthly parquet files into a single
large parquet file for easier and faster processing in PySpark.
"""

import sys
import time
import os
import subprocess
from pathlib import Path

def find_and_set_java_home():
    """Find Java installation and set JAVA_HOME"""
    
    # Check if JAVA_HOME is already set and valid
    java_home = os.environ.get('JAVA_HOME')
    if java_home and Path(java_home).exists():
        java_bin = Path(java_home) / 'bin' / 'java'
        if java_bin.exists():
            print(f"      Using existing JAVA_HOME: {java_home}")
            return True
    
    print("      JAVA_HOME not set, attempting to find Java...")
    
    # Try to find java executable
    try:
        result = subprocess.run(['which', 'java'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        java_path = result.stdout.strip()
        
        if java_path:
            # Follow symlinks and go up to JAVA_HOME
            java_path = Path(java_path).resolve()
            java_home = java_path.parent.parent
            
            if (java_home / 'bin' / 'java').exists():
                os.environ['JAVA_HOME'] = str(java_home)
                print(f"      Found Java at: {java_home}")
                return True
    
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Try common Java installation locations
    common_paths = [
        '/usr/lib/jvm/java-11-openjdk-amd64',
        '/usr/lib/jvm/java-11-openjdk',
        '/usr/lib/jvm/java-11',
        '/usr/lib/jvm/default-java',
        '/usr/lib/jvm/java-8-openjdk-amd64',
        '/usr/lib/jvm/java-8-openjdk',
        '/usr/java/latest',
        '/usr/java/default',
    ]
    
    for path in common_paths:
        if Path(path).exists():
            java_bin = Path(path) / 'bin' / 'java'
            if java_bin.exists():
                os.environ['JAVA_HOME'] = path
                print(f"      Found Java at: {path}")
                return True
    
    return False

# Try to find and set JAVA_HOME before importing PySpark
if not find_and_set_java_home():
    print("\nERROR: Java not found!")
    print("\nPySpark requires Java 8 or 11 to be installed.")
    print("\nPlease install Java:")
    print("  Ubuntu/Debian: sudo apt-get install openjdk-11-jdk")
    print("  macOS: brew install openjdk@11")
    print("\nOr set JAVA_HOME manually:")
    print("  export JAVA_HOME=/path/to/java")
    sys.exit(1)

from pyspark.sql import SparkSession

# Directories
DATA_DIR = Path("./data")
OUTPUT_FILE = DATA_DIR / "yellow_tripdata_combined.parquet"

def format_bytes(bytes_size):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def get_file_size(filepath):
    """Get size of a file or directory"""
    if filepath.is_file():
        return filepath.stat().st_size
    elif filepath.is_dir():
        return sum(f.stat().st_size for f in filepath.rglob('*') if f.is_file())
    return 0

def main():
    print("="*70)
    print("NYC Taxi Data Merger")
    print("="*70)
    print("\nThis script will combine all monthly parquet files into one.")
    print("")
    
    # Check if data directory exists
    if not DATA_DIR.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        print("Please run 'python download_data.py' first.")
        sys.exit(1)
    
    # Find all parquet files
    parquet_files = sorted(DATA_DIR.glob("yellow_tripdata_*.parquet"))
    parquet_files = [f for f in parquet_files if f.name != "yellow_tripdata_combined.parquet"]
    
    if not parquet_files:
        print(f"ERROR: No parquet files found in {DATA_DIR}")
        print("Please run 'python download_data.py' first.")
        sys.exit(1)
    
    print(f"Found {len(parquet_files)} parquet file(s) to merge:")
    total_size = 0
    for f in parquet_files:
        size = get_file_size(f)
        total_size += size
        print(f"  - {f.name:45s} {format_bytes(size):>10s}")
    
    print(f"\nTotal input size: {format_bytes(total_size)}")
    print(f"Output file: {OUTPUT_FILE}")
    
    # Check if output already exists
    if OUTPUT_FILE.exists():
        print(f"\nWARNING: Output file already exists!")
        response = input("Do you want to overwrite it? (y/N): ")
        if response.lower() != 'y':
            print("Merge cancelled.")
            sys.exit(0)
        # Remove existing output
        if OUTPUT_FILE.is_dir():
            import shutil
            shutil.rmtree(OUTPUT_FILE)
        else:
            OUTPUT_FILE.unlink()
    
    print("\n" + "="*70)
    print("Starting merge process...")
    print("="*70)
    
    # Initialize Spark
    print("\n[1/4] Initializing Spark...")
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Merger") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    print(f"      Spark {spark.version} initialized")
    print(f"      Spark UI: {spark.sparkContext.uiWebUrl}")
    
    try:
        # Read all parquet files
        print("\n[2/4] Reading parquet files...")
        start_time = time.time()
        
        # Create path pattern for all files
        input_pattern = str(DATA_DIR / "yellow_tripdata_[0-9]*.parquet")
        
        df = spark.read.parquet(*[str(f) for f in parquet_files])
        
        read_time = time.time() - start_time
        print(f"      Read completed in {read_time:.2f}s")
        
        # Get record count
        print("\n[3/4] Counting records...")
        start_time = time.time()
        record_count = df.count()
        count_time = time.time() - start_time
        print(f"      Total records: {record_count:,}")
        print(f"      Count completed in {count_time:.2f}s")
        
        # Show schema
        print("\n      Schema:")
        df.printSchema()
        
        # Write combined file
        print("\n[4/4] Writing combined parquet file...")
        print("      This may take several minutes...")
        start_time = time.time()
        
        # Repartition for optimal file size (aim for ~1GB per partition)
        # Estimate: ~50-60 million records = ~20-25GB uncompressed
        # Use 20-30 partitions for good parallelism
        num_partitions = max(20, record_count // 2_000_000)
        
        df.repartition(num_partitions).write \
            .mode("overwrite") \
            .parquet(str(OUTPUT_FILE))
        
        write_time = time.time() - start_time
        print(f"      Write completed in {write_time:.2f}s")
        
        # Get output size
        output_size = get_file_size(OUTPUT_FILE)
        compression_ratio = (total_size / output_size) if output_size > 0 else 1
        
        print("\n" + "="*70)
        print("MERGE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"\nInput files:  {len(parquet_files)} files, {format_bytes(total_size)}")
        print(f"Output file:  {OUTPUT_FILE}")
        print(f"Output size:  {format_bytes(output_size)}")
        print(f"Compression:  {compression_ratio:.2f}x")
        print(f"Records:      {record_count:,}")
        print(f"Partitions:   {num_partitions}")
        print(f"\nTotal time:   {read_time + count_time + write_time:.2f}s")
        print("\n" + "="*70)
        print("Next steps:")
        print("  1. Update notebook to use: 'data/yellow_tripdata_combined.parquet'")
        print("  2. Run: make run")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR during merge: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Stop Spark
        spark.stop()
        print("\nSpark session stopped.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMerge cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)