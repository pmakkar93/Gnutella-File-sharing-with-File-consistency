@echo off
set path=%PATH%;C:\Program Files\Java\jdk1.6.0_14\bin
echo "Setting the Class path"

echo "Checking all the configurations before setting up"
TIMEOUT /T 5
echo "Running the Server..."
java -cp GnutellaP2P_win.jar;javax.ws.rs-api-2.0.jar com.gfiletransfer.SuperPeer
pause