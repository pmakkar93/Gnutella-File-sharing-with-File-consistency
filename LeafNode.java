package com.gfiletransfer;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Scanner;

//This contains the main method for setting up the Peer.
public class LeafNode {

	public static void main(String[] args) throws RemoteException {
		Scanner sc = new Scanner(System.in); 
		String portno=null;
		String directoryName = null;
		String superPeerId =null;
		String cachedDirectoryName = null;
		
		System.out.println("Enter Peer ID ");
	    String peerId = sc.nextLine();

		// Reading Port details from property file for Instantiating Leaf Node
	    SetupConfig scg;
		try {
			scg = new SetupConfig();
			// Getting Calling Super Peer Port number
			for (GetPeerDetails p : scg.arrPD){
				if(p.getPeer_ID().equalsIgnoreCase(peerId)){
					portno = p.getPeer_Port();
					directoryName=p.getDir();
					cachedDirectoryName = p.getCacheDir();
					superPeerId=p.getSuperPeer();
					break;
				}
			}			 
		}
		catch (IOException e1) {
			System.out.println("IOException occured while reading the property file at Leaf Node Initialization.");
		}
	    
	     // Registering the peer on specified port & setting up the remote object
    	 Registry registry = LocateRegistry.createRegistry(Integer.parseInt(portno));
    	 /*--------- start change ----------*/
    	 LeafNodeImpl lnImpl = new LeafNodeImpl(portno, directoryName, superPeerId,peerId,cachedDirectoryName);
    	 /*--------- end change ----------*/
    	 LeafNodeInterface lnInter = (LeafNodeInterface)UnicastRemoteObject.exportObject(lnImpl, 0);
		 registry.rebind("root://LeafNode/"+portno+"/FS", lnInter);
		 System.out.println("Peer is up and Running.");
		 try {
			 lnImpl.doWork();
		} catch (IOException e) {
			System.out.println("IO Exception at Leaf Node Main" + e.getMessage());
			e.printStackTrace();
		} 
}	
}
