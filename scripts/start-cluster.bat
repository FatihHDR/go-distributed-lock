@echo off
REM Start 3-node distributed lock cluster

echo Starting Distributed Lock Cluster (3 nodes)
echo.

REM Build the demo first
go build -o demo.exe ./cmd/demo
if %ERRORLEVEL% neq 0 (
    echo Build failed!
    exit /b 1
)

echo Starting node-1 on port 9001 (will be leader)...
start "Node-1" cmd /k "demo.exe -mode node -id node-1 -port 9001 -bootstrap -peers node-2:127.0.0.1:9002,node-3:127.0.0.1:9003"

timeout /t 2 /nobreak > nul

echo Starting node-2 on port 9002...
start "Node-2" cmd /k "demo.exe -mode node -id node-2 -port 9002 -peers node-1:127.0.0.1:9001,node-3:127.0.0.1:9003"

timeout /t 1 /nobreak > nul

echo Starting node-3 on port 9003...
start "Node-3" cmd /k "demo.exe -mode node -id node-3 -port 9003 -peers node-1:127.0.0.1:9001,node-2:127.0.0.1:9002"

echo.
echo All 3 nodes started!
echo.
echo To test, run in another terminal:
echo   demo.exe -mode client -port 9001
echo.
echo Or run the interactive demo:
echo   demo.exe -mode demo
