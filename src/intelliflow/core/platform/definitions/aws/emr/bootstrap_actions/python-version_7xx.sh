#!/bin/bash
set -x

# AWS Support suggested to do action post-provisioning (so poll till the end of provisioning)
# EMR case: 174300818400359

# Set up logging early
exec > >(sudo tee /var/log/python-bootstrap.log)
exec 2>&1

echo "Starting bootstrap script at $(date)"

# Initialize polling variables
MAX_POLL_ATTEMPTS=12  # 2 minutes with 10-second intervals
POLL_COUNT=0
NODEPROVISIONSTATE=""

# Poll for node provisioning state before proceeding with Python installation
while [ $POLL_COUNT -lt $MAX_POLL_ATTEMPTS ]; do
    POLL_COUNT=$((POLL_COUNT + 1))
    
    # Check if the state file exists
    if [ ! -f "/emr/instance-controller/lib/info/job-flow-state.txt" ]; then
        echo "Attempt $POLL_COUNT/$MAX_POLL_ATTEMPTS: State file not found, waiting..."
        sleep 10
        continue
    fi
    
    NODEPROVISIONSTATE=` sed -n '/localInstance [{]/,/[}]/{
/nodeProvisionCheckinRecord [{]/,/[}]/ {
   /status: / { p }
    /[}]/a
   }
  /[}]/a
}'  /emr/instance-controller/lib/info/job-flow-state.txt | awk ' { print $2 }'`

    echo "Attempt $POLL_COUNT/$MAX_POLL_ATTEMPTS: Node provision state = '$NODEPROVISIONSTATE'"
    
    if [ "$NODEPROVISIONSTATE" == "SUCCESSFUL" ]; then
        echo "Node provisioning successful, proceeding with Python installation..."
        break
    elif [ "$NODEPROVISIONSTATE" == "FAILED" ] || [ "$NODEPROVISIONSTATE" == "TERMINATED" ]; then
        echo "Node provisioning failed with state: $NODEPROVISIONSTATE"
        exit 1
    fi
    
    sleep 10
done

# Check if we timed out or if provisioning was successful
if [ $POLL_COUNT -ge $MAX_POLL_ATTEMPTS ]; then
    echo "Timeout reached after $MAX_POLL_ATTEMPTS attempts. Last state: '$NODEPROVISIONSTATE'"
    echo "Proceeding with Python installation anyway..."
fi

# Proceed with Python installation
if [ "$NODEPROVISIONSTATE" == "SUCCESSFUL" ] || [ $POLL_COUNT -ge $MAX_POLL_ATTEMPTS ]; then
    sleep 10;
    echo "Starting Python installation at $(date)"

    # Function for retrying commands
    retry_command() {
        local max_attempts=3
        local delay=5
        local attempt=1
        while true; do
            "$@" && break || {
                if [[ $attempt -lt $max_attempts ]]; then
                    echo "Command failed. Attempt $attempt/$max_attempts. Retrying in $delay seconds..."
                    ((attempt++))
                    sleep $delay
                else
                    echo "Command failed after $max_attempts attempts. Exiting..."
                    return 1
                fi
            }
        done
    }

    # Check available disk space
    if [ $(df -P / | awk 'NR==2 {print $4}') -lt 2097152 ]; then
        echo "Less than 2GB free space available. Aborting."
        exit 1
    fi

    # Install dependencies (including boost libraries for rodb native extensions)
    retry_command sudo yum install -y libffi-devel openssl-devel bzip2-devel xz-devel gcc sqlite-devel
    retry_command sudo yum install -y boost-devel boost-python3-devel boost-system boost-filesystem

    # Download and extract Python
    retry_command sudo wget https://www.python.org/ftp/python/$1.$2.$3/Python-$1.$2.$3.tgz
    retry_command sudo tar -zxvf Python-$1.$2.$3.tgz
    cd Python-$1.$2.$3 || exit 1

    # Configure and install Python
    sudo ./configure --enable-optimizations --with-ensurepip=install
    timeout 30m sudo make install || echo "Make install timed out after 30 minutes"

    # Install packages
    retry_command python$1.$2 -m pip install --upgrade awscli --user
    retry_command python$1.$2 -m pip install ctypes

    # Set up symbolic links carefully to avoid breaking system Python
    # Only create the specific version symlink if it doesn't already exist
    if [ ! -e "/usr/bin/python$1.$2" ]; then
        sudo ln -sf /usr/local/bin/python$1.$2 /usr/bin/python$1.$2
        echo "Created symlink: /usr/bin/python$1.$2 -> /usr/local/bin/python$1.$2"
    else
        echo "Symlink /usr/bin/python$1.$2 already exists, skipping"
    fi
    
    # DO NOT override /usr/bin/python3 as it may break system package management
    # Instead, just ensure our custom Python is available in PATH
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/lib64
    
    # Diagnose boost::python library versions to identify compatibility issues
    echo "=== Boost::Python Library Diagnostic ==="
    echo "Target Python version: $1.$2"
    echo "System Python versions available:"
    ls -la /usr/bin/python* 2>/dev/null | grep -E 'python[0-9]' || echo "None found"
    
    echo "Default system Python version:"
    python3 --version 2>/dev/null || echo "python3 not found"
    
    echo "Searching for boost::python libraries on system..."
    find /usr/lib* -name "*boost_python*" -type f 2>/dev/null | while read lib; do
        echo "Found: $lib"
        # Extract version from filename if possible
        if [[ "$lib" =~ libboost_python([0-9]+)\.so ]]; then
            echo "  Detected Python version in filename: ${BASH_REMATCH[1]}"
        elif [[ "$lib" =~ libboost_python([0-9])([0-9]+)\.so ]]; then
            echo "  Detected Python version in filename: ${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
        fi
        
        # Try to get more info about the library
        if command -v readelf >/dev/null 2>&1; then
            echo "  Library info:"
            readelf -d "$lib" 2>/dev/null | grep -E '(SONAME|NEEDED)' | head -5
        fi
    done
    
    # Check what Python the system boost was likely compiled for
    echo "Checking system Python configuration..."
    if [ -f "/usr/lib64/libboost_python38.so" ]; then
        echo "RECOMMENDATION: System has libboost_python38.so - consider using Python 3.8"
    elif [ -f "/usr/lib64/libboost_python39.so" ]; then
        echo "RECOMMENDATION: System has libboost_python39.so - current Python 3.9 should be compatible"
    elif [ -f "/usr/lib64/libboost_python310.so" ]; then
        echo "RECOMMENDATION: System has libboost_python310.so - consider using Python 3.10"
    elif [ -f "/usr/lib64/libboost_python311.so" ]; then
        echo "RECOMMENDATION: System has libboost_python311.so - consider using Python 3.11"
    else
        echo "WARNING: No version-specific boost::python library found, this may cause compatibility issues"
    fi
    
    echo "Installed Python version: /usr/local/bin/python$1.$2"
    
    # Configure library paths with /tmp having highest priority for rodb native libraries
    # This ensures rodb-provided boost::python library overrides system versions
    echo "export LD_LIBRARY_PATH=/tmp:\$LD_LIBRARY_PATH:/usr/local/lib:/usr/lib64" | sudo tee -a /etc/spark/conf/spark-env.sh
    echo "export PYTHONPATH=/usr/local/lib/python$1.$2/site-packages:\$PYTHONPATH" | sudo tee -a /etc/spark/conf/spark-env.sh

    # Set PySpark environment variables to use our specific Python version
    echo "export PYSPARK_PYTHON=/usr/local/bin/python$1.$2" | sudo tee -a /etc/spark/conf/spark-env.sh
    echo "export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python$1.$2" | sudo tee -a /etc/spark/conf/spark-env.sh
    
    # Force rodb boost library to take precedence over system boost
    # The rodb flow downloads its own libboost_python{VERSION}.so to /tmp
    # We need to ensure this takes precedence over system libraries
    echo "export LD_PRELOAD=\"/tmp/libboost_python$1$2.so:\$LD_PRELOAD\"" | sudo tee -a /etc/spark/conf/spark-env.sh
    
    echo "=== End Boost::Python Diagnostic ==="

    echo "Python installation completed at $(date)"

    exit 0
else
    echo "Skipping Python installation due to provisioning state: $NODEPROVISIONSTATE"
    exit 1
fi
