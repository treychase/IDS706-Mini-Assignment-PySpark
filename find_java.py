#!/usr/bin/env python3
"""
Detect Java installation and set JAVA_HOME
"""

import os
import subprocess
import sys
from pathlib import Path

def find_java():
    """Find Java installation on the system"""
    
    # Check if JAVA_HOME is already set
    java_home = os.environ.get('JAVA_HOME')
    if java_home and Path(java_home).exists():
        java_bin = Path(java_home) / 'bin' / 'java'
        if java_bin.exists():
            return java_home
    
    # Try to find java executable
    try:
        # Use 'which java' on Unix-like systems
        result = subprocess.run(['which', 'java'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        java_path = result.stdout.strip()
        
        if java_path:
            # Follow symlinks
            java_path = Path(java_path).resolve()
            
            # Go up two directories to get JAVA_HOME
            # /usr/lib/jvm/java-11-openjdk-amd64/bin/java -> /usr/lib/jvm/java-11-openjdk-amd64
            java_home = java_path.parent.parent
            
            # Verify it looks like a valid JAVA_HOME
            if (java_home / 'bin' / 'java').exists():
                return str(java_home)
    
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
        '/Library/Java/JavaVirtualMachines',  # macOS
    ]
    
    for path in common_paths:
        if Path(path).exists():
            java_bin = Path(path) / 'bin' / 'java'
            if java_bin.exists():
                return path
            
            # For macOS, might need to look deeper
            if 'JavaVirtualMachines' in path:
                for jvm_dir in Path(path).iterdir():
                    if jvm_dir.is_dir():
                        contents = jvm_dir / 'Contents' / 'Home'
                        if contents.exists() and (contents / 'bin' / 'java').exists():
                            return str(contents)
    
    return None

def main():
    java_home = find_java()
    
    if java_home:
        print(java_home)
        return 0
    else:
        print("ERROR: Java not found", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())