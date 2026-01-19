#!/bin/bash
# Start 3-node distributed lock cluster

echo "Starting Distributed Lock Cluster (3 nodes)"
echo ""

# Build the demo first
go build -o demo ./cmd/demo
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Starting node-1 on port 9001 (will be leader)..."
./demo -mode node -id node-1 -port 9001 -bootstrap \
    -peers node-2:127.0.0.1:9002,node-3:127.0.0.1:9003 &

sleep 2

echo "Starting node-2 on port 9002..."
./demo -mode node -id node-2 -port 9002 \
    -peers node-1:127.0.0.1:9001,node-3:127.0.0.1:9003 &

sleep 1

echo "Starting node-3 on port 9003..."
./demo -mode node -id node-3 -port 9003 \
    -peers node-1:127.0.0.1:9001,node-2:127.0.0.1:9002 &

echo ""
echo "All 3 nodes started!"
echo ""
echo "To test, run in another terminal:"
echo "  ./demo -mode client -port 9001"
echo ""
echo "Or run the interactive demo:"
echo "  ./demo -mode demo"
echo ""
echo "Press Ctrl+C to stop all nodes..."

# Wait for interrupt
trap 'kill $(jobs -p); exit' INT
wait
