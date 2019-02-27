@echo off
set path=%PATH%;C:\Program Files\Java\jdk1.6.0_14\bin
echo "Setting the Class path"

echo "Running the Peer..."
TIMEOUT /T 3
echo "#################"
echo "Peer is now Running. Please configure your setting."
echo "#################"
java -cp GnutellaP2P_win.jar;javax.ws.rs-api-2.0.jar com.gfiletransfer.LeafNode
pause