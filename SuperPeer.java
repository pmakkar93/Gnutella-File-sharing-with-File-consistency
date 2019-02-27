package com.gfiletransfer;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

public class SuperPeer {

	public static void main(String[] args)  throws RemoteException{

		Scanner sc = new Scanner(System.in); 
		String peerID = null;
		String portNum = null;
	    System.out.println("Enter the Super Peer ID.");   
	    peerID =sc.nextLine();

		// Reading Port details from property file for Instantiating Super Peer
	    SetupConfig scg;
		try {
			scg = new SetupConfig();
			// Getting Calling Super Peer Port number
			for (GetSuperPeerDetails sp : scg.arrSPD){
				if(sp.getPeer_ID().equalsIgnoreCase(peerID)){
					portNum = sp.getPeer_Port();
					break;
				}
			}			 
		}
		catch (IOException e1) {
			System.out.println("IOException occured while reading the property file at SuperPeer Initialization.");
		}
			 Registry registry = LocateRegistry.createRegistry(Integer.parseInt(portNum));
			 SuperPeerImpl spImpl = new SuperPeerImpl();
			 SuperPeerInterface spInter = (SuperPeerInterface)UnicastRemoteObject.exportObject(spImpl, 0);
			 registry.rebind("root://SuperPeer/"+portNum, spInter);
			 System.out.println("Server now is up and Running.");
			 
	}

}
