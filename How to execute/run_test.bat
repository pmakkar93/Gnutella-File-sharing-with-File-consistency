@echo off
set path=%PATH%;C:\Program Files\Java\jdk1.6.0_14\bin
echo "Setting the Class path"

echo "For Evaluation of Peer to Peer File transfering system, we are running multiple clients at once..."
echo "Starting to set up all configurations. "
TIMEOUT /T 3
echo "#############################"
echo "LOAD TESTING of P2P started"
echo "#############################"

java -cp GnutellaP2P_win.jar;javax.ws.rs-api-2.0.jar com.gfiletransfer.MultiClient
pause