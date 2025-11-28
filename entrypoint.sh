#!/bin/bash

# 1. Start the Web Server in the background
# We assume the config points output to /data, so we serve /data
echo "Starting Web Server..."
python3 serve_results.py --dir /data &

# 2. Start the Detection Loop
echo "Starting Detection Loop..."

while true; do
    echo "[$(date)] Running Flaky Test Detector..."

    # Run the python script
    # We rely on the mounted config.yaml at /app/config.yaml
    python3 flaky_detector.py --config /app/config.yaml

    # Calculate exit code to prevent crash loops if script fails
    if [ $? -ne 0 ]; then
        echo "Detector script failed. Retrying in next interval."
    fi

    # Sleep for 4 hours (14400 seconds)
    # You can override this via Docker ENV var $CHECK_INTERVAL
    SLEEP_TIME=${CHECK_INTERVAL:-14400}
    echo "Sleeping for $SLEEP_TIME seconds..."
    sleep $SLEEP_TIME
done
