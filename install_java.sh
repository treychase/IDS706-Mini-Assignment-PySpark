#!/bin/bash
# Install Java 17 for PySpark compatibility

echo "=========================================="
echo "Installing Java 17 for PySpark"
echo "=========================================="
echo ""

# Check current Java version
if command -v java &> /dev/null; then
    echo "Current Java version:"
    java -version
    echo ""
fi

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS=$(uname -s)
fi

echo "Detected OS: $OS"
echo ""

case $OS in
    ubuntu|debian)
        echo "Installing Java 17 on Ubuntu/Debian..."
        echo ""
        
        # Update package list
        echo "Updating package list..."
        sudo apt-get update
        
        # Install Java 17
        echo ""
        echo "Installing OpenJDK 17..."
        sudo apt-get install -y openjdk-17-jdk
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✓ Java 17 installed successfully!"
            
            # Set JAVA_HOME
            JAVA_PATH="/usr/lib/jvm/java-17-openjdk-amd64"
            
            # Check if the path exists, if not try alternative
            if [ ! -d "$JAVA_PATH" ]; then
                JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java))))
            fi
            
            echo ""
            echo "Setting JAVA_HOME..."
            
            # Add to current session
            export JAVA_HOME=$JAVA_PATH
            export PATH=$JAVA_HOME/bin:$PATH
            
            # Add to .bashrc for persistence
            if ! grep -q "JAVA_HOME.*java-17" ~/.bashrc; then
                echo "" >> ~/.bashrc
                echo "# Java 17 for PySpark" >> ~/.bashrc
                echo "export JAVA_HOME=$JAVA_PATH" >> ~/.bashrc
                echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
            fi
            
            echo "✓ JAVA_HOME set to: $JAVA_HOME"
            echo ""
            echo "New Java version:"
            java -version
            
            echo ""
            echo "=========================================="
            echo "Installation Complete!"
            echo "=========================================="
            echo ""
            echo "Java 17 is now installed and configured."
            echo "Run your PySpark commands now!"
        else
            echo "✗ Installation failed!"
            exit 1
        fi
        ;;
        
    darwin)
        echo "Installing Java 17 on macOS..."
        echo ""
        if command -v brew &> /dev/null; then
            echo "Installing OpenJDK 17 via Homebrew..."
            brew install openjdk@17
            
            if [ $? -eq 0 ]; then
                echo ""
                echo "✓ Java 17 installed successfully!"
                
                # Link for system Java wrappers
                sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
                
                JAVA_PATH="$(brew --prefix)/opt/openjdk@17"
                
                # Add to .zshrc
                if ! grep -q "JAVA_HOME.*openjdk@17" ~/.zshrc; then
                    echo "" >> ~/.zshrc
                    echo "# Java 17 for PySpark" >> ~/.zshrc
                    echo "export JAVA_HOME=$JAVA_PATH" >> ~/.zshrc
                    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.zshrc
                fi
                
                export JAVA_HOME=$JAVA_PATH
                export PATH=$JAVA_HOME/bin:$PATH
                
                echo "✓ JAVA_HOME set to: $JAVA_HOME"
                echo ""
                echo "New Java version:"
                java -version
            else
                echo "✗ Installation failed!"
                exit 1
            fi
        else
            echo "Homebrew not found!"
            echo "Please install Homebrew first: https://brew.sh"
            echo "Or download Java 17 manually: https://adoptium.net/"
            exit 1
        fi
        ;;
        
    *)
        echo "Unsupported OS: $OS"
        echo ""
        echo "Please install Java 17 manually:"
        echo "  - Download from: https://adoptium.net/temurin/releases/?version=17"
        echo "  - Or use your system's package manager"
        exit 1
        ;;
esac

echo ""
echo "Next steps:"
echo "1. Run: source ~/.bashrc  (or restart your terminal)"
echo "2. Verify: java -version  (should show version 17)"
echo "3. Run: make merge-data"