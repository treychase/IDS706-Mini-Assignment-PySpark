#!/bin/bash
# Java Installation Helper for PySpark

echo "=========================================="
echo "Java Installation Helper"
echo "=========================================="
echo ""

# Check if Java is already installed
if command -v java &> /dev/null; then
    echo "✓ Java is already installed:"
    java -version
    echo ""
    echo "If PySpark still doesn't work, you may need to set JAVA_HOME:"
    
    # Try to find Java home
    if [ -d "/usr/lib/jvm/default-java" ]; then
        JAVA_PATH="/usr/lib/jvm/default-java"
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        JAVA_PATH="/usr/lib/jvm/java-11-openjdk-amd64"
    else
        JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java))))
    fi
    
    echo ""
    echo "Add this to your ~/.bashrc or ~/.zshrc:"
    echo "  export JAVA_HOME=$JAVA_PATH"
    echo "  export PATH=\$JAVA_HOME/bin:\$PATH"
    echo ""
    echo "Then run: source ~/.bashrc"
    exit 0
fi

echo "Java is not installed. Let's install it!"
echo ""

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
        echo "Installing Java on Ubuntu/Debian..."
        echo ""
        echo "Running: sudo apt-get update"
        sudo apt-get update
        
        echo ""
        echo "Running: sudo apt-get install -y default-jdk"
        sudo apt-get install -y default-jdk
        
        if [ $? -eq 0 ]; then
            echo ""
            echo "✓ Java installed successfully!"
            java -version
            
            # Set JAVA_HOME
            JAVA_PATH="/usr/lib/jvm/default-java"
            echo ""
            echo "Setting JAVA_HOME..."
            echo "export JAVA_HOME=$JAVA_PATH" >> ~/.bashrc
            echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
            
            export JAVA_HOME=$JAVA_PATH
            export PATH=$JAVA_HOME/bin:$PATH
            
            echo "✓ JAVA_HOME set to: $JAVA_HOME"
            echo ""
            echo "To make it permanent, restart your terminal or run:"
            echo "  source ~/.bashrc"
        else
            echo "✗ Installation failed!"
            exit 1
        fi
        ;;
        
    darwin)
        echo "Installing Java on macOS..."
        echo ""
        if command -v brew &> /dev/null; then
            echo "Running: brew install openjdk@11"
            brew install openjdk@11
            
            if [ $? -eq 0 ]; then
                echo ""
                echo "✓ Java installed successfully!"
                
                # Link for system Java wrappers
                sudo ln -sfn $(brew --prefix)/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
                
                JAVA_PATH="$(brew --prefix)/opt/openjdk@11"
                echo "export JAVA_HOME=$JAVA_PATH" >> ~/.zshrc
                echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.zshrc
                
                export JAVA_HOME=$JAVA_PATH
                export PATH=$JAVA_HOME/bin:$PATH
                
                echo "✓ JAVA_HOME set to: $JAVA_HOME"
                echo ""
                echo "To make it permanent, restart your terminal or run:"
                echo "  source ~/.zshrc"
            else
                echo "✗ Installation failed!"
                exit 1
            fi
        else
            echo "Homebrew not found!"
            echo "Please install Homebrew first: https://brew.sh"
            echo "Or download Java manually: https://adoptium.net/"
            exit 1
        fi
        ;;
        
    *)
        echo "Unsupported OS: $OS"
        echo ""
        echo "Please install Java manually:"
        echo "  - Download from: https://adoptium.net/"
        echo "  - Or use your system's package manager"
        echo ""
        echo "After installation, set JAVA_HOME:"
        echo "  export JAVA_HOME=/path/to/java"
        echo "  export PATH=\$JAVA_HOME/bin:\$PATH"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Restart your terminal or run: source ~/.bashrc"
echo "2. Verify Java: java -version"
echo "3. Run download script: python download_data_fixed.py"