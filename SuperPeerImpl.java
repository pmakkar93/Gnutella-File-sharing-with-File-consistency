package com.gfiletransfer;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit; 

public class SuperPeerImpl implements SuperPeerInterface {
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    //get current date time with Date()
    Date date = new Date();

	// Defining a hash map for indexing the file details
    // We are using MultiValue Hash Map Data Structure for storing Multiple Entries for a single filename. 
    // For each Filename(Key), there will be collection of entries(Value).
	private MultivaluedMap<String, ArrayList<String>> fileDictionary = new MultivaluedHashMap<>();
	// Buffer for storing the requests
	private Map<String,ArrayList<String>> myMap = new HashMap<String,ArrayList<String>>();
	private String supPeerId=null;
	/*--------- start change ----------*/
	public static  Map<String,String> flagTable = new HashMap<String,String>();
	public static  Map<String,String> status1Table = new HashMap<String,String>();
	final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(15); // no
	static ScheduledFuture<?> t;
	/*--------- end change ----------*/
	
	/*--------- start change ----------*/
	@Override
	public void registryFiles(String rd, String filename, String peerid, String port_num, String directory,
			String sPeer, String copyType, String versionNum, String status, String lastModTime, String TTR, String ogPeerId)
			throws RemoteException {
		/*--------- end change ----------*/
		// Indexing the File details -- "new"
		if(rd.equalsIgnoreCase("new")){
			// checking for duplicate record in Index
			if(this.fileDictionary.containsKey(filename)){
				Collection<ArrayList<String>> delArrFile = this.fileDictionary.get(filename);
				for(ArrayList<String> als : delArrFile){
					if(als.get(1).equalsIgnoreCase(peerid)){
						// Duplicate record for Indexing. so Rejecting it
						return;
					}
				}
			}
			// If no duplicate, then indexing it
			// 0 - filename,1-peerid,2-portno,3-direct,4-superpeerid,5-copytype,6-vernum,7-status,8-lmt,9-TTR,10-ogPeerId
			ArrayList<String> arrFileDtl = new ArrayList<String>();
			arrFileDtl.add(filename);
			arrFileDtl.add(peerid);
			arrFileDtl.add(port_num);
			arrFileDtl.add(directory);
			arrFileDtl.add(sPeer);
			arrFileDtl.add(copyType);
			arrFileDtl.add(versionNum);
			arrFileDtl.add(status);
			arrFileDtl.add(lastModTime);
			arrFileDtl.add(TTR);
			arrFileDtl.add(ogPeerId);
			//System.out.println(arrFileDtl);
			this.fileDictionary.add(filename, arrFileDtl);
			this.supPeerId = sPeer;
		}
		
		// Updating the Index after deletion of a file -- "del"
		else if(rd.equalsIgnoreCase("del")){
			Collection<ArrayList<String>> delArrFile = new ArrayList<ArrayList<String>>();
			Collection<ArrayList<String>> onceMore = new ArrayList<ArrayList<String>>();
			
			// Checking if deleted File Name is present in Index
			if(this.fileDictionary.containsKey(filename)){
				delArrFile = this.fileDictionary.get(filename);
				for(ArrayList<String> als : delArrFile){
					// Removing the Peer Entry from Index
					if(als.get(1).equalsIgnoreCase(peerid)){
						//System.out.println(this.fileDictionary.get(filename));
						onceMore= this.fileDictionary.remove(filename);
						//System.out.println("BEFORE"+onceMore);
						onceMore.remove(als);
						//System.out.println("AFTER" + onceMore);
						for (ArrayList<String> p : onceMore){
							this.fileDictionary.add(filename,p);							
						}
						System.out.println("Index Server Updated & Specified Record Deleted");
					}
				}
			}
			else{
				System.out.println("Delete Request: No entry detected for filename");
			}
		}
		/*--------- start change ----------*/
		// 0 - filename,1-peerid,2-portno,3-direct,4-superpeerid,5-copytype,6-vernum,7-status,8-lmt,9-TTR,10-ogPeerId
		else if(rd.equalsIgnoreCase("upd")){
			Collection<ArrayList<String>> updArrFile = new ArrayList<ArrayList<String>>();
			Collection<ArrayList<String>> onceMore = new ArrayList<ArrayList<String>>();
			
			// Checking if edited File Name is present in Index
			if(this.fileDictionary.containsKey(filename)){
				updArrFile = this.fileDictionary.get(filename);
				for(ArrayList<String> als : updArrFile){
					// updating the Peer Entry from Index
					if(als.get(1).equalsIgnoreCase(peerid)){
						ArrayList<String> up = als;
						up.set(6, versionNum);
						up.set(8, lastModTime);
						up.set(7, status);
						
						onceMore= this.fileDictionary.remove(filename);
						onceMore.remove(als);
						onceMore.add(up);
						
						for (ArrayList<String> p : onceMore){
							this.fileDictionary.add(filename,p);							
						}
						break;
					}
				}
				System.out.println("Index Server Updated & Specified Record Updated");
			}
			else{
				System.out.println("Update Request: No entry detected for filename under requested Peer");
			}			
		}
		/*--------- end change ----------*/
		else{
			System.out.println("Invalid Request.");
		}
		// Displaying the Index after every Addition or Removal of Entry.
		System.out.println("####################################");
		System.out.println("THE UPDATED INDEX at " + dateFormat.format(date));
		for (Entry<String, List<ArrayList<String>>> entry : this.fileDictionary.entrySet()) {
		    System.out.println(entry.getKey() + " => " + entry.getValue());
		}
		System.out.println("####################################");
	}

	// Searching specified Filename entry from Index and returns a Collection of ArrayList
	@Override
	public synchronized Collection<ArrayList<String>> searchFile(String filename) throws RemoteException {
		// TODO Auto-generated method stub
		Collection<ArrayList<String>> resultArrFile = new ArrayList<ArrayList<String>>();
		if(this.fileDictionary.containsKey(filename)){
			resultArrFile = this.fileDictionary.get(filename);
		}
		return resultArrFile;
	}

	@Override
	// This Query function is for All to All Topology and linear Topology
	public void query(String msgId, int TTL, String filename, String reqPeerId, String reqPortNum)
			throws RemoteException {
		if(TTL>0 && TTL != 0){
			TTL=TTL-1;
			// Inserting request details into HashMap at Server
			ArrayList<String> upStreamDtl = new ArrayList<String>();
			upStreamDtl.add(msgId);
			upStreamDtl.add(Integer.toString(TTL));
			upStreamDtl.add(reqPeerId);
			upStreamDtl.add(reqPortNum);
			this.myMap.put(msgId, upStreamDtl);

			// displaying all the request details 
			for (Entry<String, ArrayList<String>> entry : this.myMap.entrySet()) {
			    System.out.println(entry.getKey() + " => " + entry.getValue());
			}
			// Searching the requested file
			Collection<ArrayList<String>> resultLocal = this.searchFile(filename);
			if(!resultLocal.isEmpty()){
				try{
					// Locating Registry of Requested Super peer or leaf node 	
					Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(this.myMap.get(msgId).get(3)));
					String ref = msgId.substring(0, msgId.indexOf(":"));              // Message id - PeerId:SequenceNumber
					// If caller is leaf node or super node
					if (ref.equalsIgnoreCase(reqPeerId)){
						// Calling Leaf node interface methods
						LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+this.myMap.get(msgId).get(3)+"/FS");

						//System.out.println("Status from Leaf Node :" + pInter.queryHit(msgId,TTL,filename,resultLocal));
						if(pInter.queryHit(msgId,TTL,filename,resultLocal)){
							System.out.println("Output Send to Leaf Node");
						}
						else{
							System.out.println("Some exception might have occured at Leaf Node or TTL expired.");
						}
					}
					else{
						System.out.println("Calling Super Peer Caller");
						SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+this.myMap.get(msgId).get(3));
						spInter.queryHit(msgId,TTL,filename,resultLocal);
					}
				}
				catch(Exception e){
						System.out.println("Exception at its own SuperPeer query function : " + e.getMessage());
					}
			}
			else{
				System.out.println("FOUND NOTHING in this SuperPeer");
			}
			
			// Get Local and remote Super peer port number
			String remoteSupPeerPortNum = null;
			String localSupPeerPortNum = null;

			// Reading the config file.
			SetupConfig sc;
			try {
				sc = new SetupConfig();
				// Getting Calling Super Peer Port number
				for (GetSuperPeerDetails sp : sc.arrSPD){
					if(sp.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
						localSupPeerPortNum= sp.getPeer_Port();
						break;
					}
				}
				String callingLeafId = msgId.substring(0, msgId.indexOf(":"));
								
				if(sc.topology.equalsIgnoreCase("ALL")){
					System.out.println("WORKING IN ALL TO ALL TOPOLOGY");
					if(callingLeafId.equalsIgnoreCase(this.myMap.get(msgId).get(2))){
						for (GetTopologyDetails topo : sc.arrTD){
							if(topo.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
								List<String> neighbourArr = Arrays.asList(topo.getAll_Neighbour().split("\\s*,\\s*"));
								System.out.println("Total Number of Neighbours in ALL TOPOLOGY : "+ neighbourArr.size());
							
							// Calling query function of Neighbours in ALL TOPOLOGY
								for (String spName : neighbourArr){
									// Getting Remote Super Peer Port Number
									for(GetSuperPeerDetails speer : sc.arrSPD){
										// Get port number of Super Peer
										if(speer.getPeer_ID().equalsIgnoreCase(spName)){
											remoteSupPeerPortNum = speer.getPeer_Port();
											break;
										}
									}
									// Calling Neighbouring query method
										try{
											Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(remoteSupPeerPortNum));
											SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+remoteSupPeerPortNum);
											System.out.println("Calling Neighbour " + spName + " query() ");
											spInter.query(msgId, TTL, filename,this.supPeerId, localSupPeerPortNum);
										}
										catch(Exception e){
											System.out.println("Exception occured at calling Neighbour Query. Neighbour is : " + spName );
										}
								}
							break;
							}
							else{
								System.out.println("Didnt found Super Peer Info in Config file object.");
							}
						}	
					}
					else{
						System.out.println("No Need of broadcasting query messages to all Super Peers.");
					}
				}
				else{
					System.out.println("WORKING IN LINEAR TOPOLOGY");
					List<String> leafPeerIdArr = null;
					for (GetTopologyDetails topo : sc.arrTD){
						if(topo.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
							String neighbour = topo.getLinear_Neighbour();							
							System.out.println("SuperPeer " + this.supPeerId + " have Neighbour in Linear TOPOLOGY : "+ neighbour);
														
							// Getting Remote Super Peer Port Number
								String spName = neighbour;
								for(GetSuperPeerDetails speer : sc.arrSPD){
									// Get port number of Super Peer
									if(speer.getPeer_ID().equalsIgnoreCase(spName)){
										remoteSupPeerPortNum = speer.getPeer_Port();
										leafPeerIdArr = Arrays.asList(speer.getLeaf_ID().split("\\s*,\\s*"));
										break;
									}
								}
								// Calling Neighbouring query method and making sure it doesn't call Caller's Super Peer back.
								if(!leafPeerIdArr.contains(callingLeafId)){
									try{
										Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(remoteSupPeerPortNum));
										SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+remoteSupPeerPortNum);
										System.out.println("Calling Neighbour " + spName + " query() ");
										spInter.query(msgId, TTL, filename,this.supPeerId, localSupPeerPortNum);
									}
									catch(Exception e){
										System.out.println("Exception occured at calling Neighbour Query. Neighbour is : " + spName );
									}
								}
								else{
									System.out.println("No Need of forwarding query messages to Super Peers.");
								}		
							break;
						}
						else{
							System.out.println("Didnt found Super Peer Info in Config file object.");
						}
					}
					
				}
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.out.println("IOException occured while reading the property file in SuperPeer Query.");
			}
		}
		else{
			System.out.println("Time to Live of a Message has expired at its own Super Peer. This Message is no longer valid.");
		}
}

	@Override
	public synchronized void queryHit(String msgId, int TTL, String filename, Collection<ArrayList<String>> resultArr)
			throws RemoteException {
		
		if(TTL>0 && TTL != 0){
			try{
				TTL=TTL-1;
				Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(this.myMap.get(msgId).get(3)));
				String ref = msgId.substring(0, msgId.indexOf(":"));
				
				// to check whether leaf node calling has reached
				if (ref.equalsIgnoreCase(this.myMap.get(msgId).get(2))){
					// Calling Leaf node interface methods
					LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+this.myMap.get(msgId).get(3)+"/FS");

//					System.out.println("Status from Leaf Node :" + pInter.queryHit(msgId,TTL,filename,resultArr));
					if(pInter.queryHit(msgId,TTL,filename,resultArr)){
						System.out.println("Output Send to Leaf Node");
					}
					else{
						System.out.println("Some exception might have occured at Leaf Node or TTL expired.");
					}
				}
				else{
					SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+this.myMap.get(msgId).get(3));
					spInter.queryHit(msgId,TTL,filename,resultArr);
				}
			}
			catch(Exception e){
				System.out.println("Exception at Remote SuperPeer queryhit function : " + e.getMessage());
			}
		}
		else{
			System.out.println("Time to Live of a Message has expired at remote SuperNode. This Message is no longer valid.");
		}
	}

	/*--------- start change ----------*/
	@Override
	public void broadCastSP(String msgId, String filename, String originLNServer, String verNum)
			throws RemoteException {

		String remoteSupPeerPortNum = null;
		// Reading the config file.
		SetupConfig sc;
		try {
				sc = new SetupConfig();
				//Sending invalidate request to its own leaf node other than master node.
				
				List<String> apd = new ArrayList<String>();
				for (GetSuperPeerDetails sp : sc.arrSPD){
					if(sp.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
						String pd = sp.getLeaf_ID();
						apd = Arrays.asList(pd.split(","));
						break;
					}
				}
				System.out.println("BEFORE Leaf nodes under this super peer is : " +apd +" ORIGIN SERVER "+ originLNServer);
				
				List<String> apdUpdated = new ArrayList<String>();
				for(String s: apd){
					if(s.equalsIgnoreCase(originLNServer)){
						continue;
					}
					apdUpdated.add(s);
				}
				apd=apdUpdated;
				System.out.println("AFTER Leaf nodes under this super peer is : " +apd);
				
				//extract port numbers from config file after comparing leaf IDs in array list and config file
				for(String ls: apd) {
					for(GetPeerDetails p : sc.arrPD) {
						if ( p.getPeer_ID().equalsIgnoreCase(ls)) {
							String Port_No = p.getPeer_Port();
								Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(Port_No));
								LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+Integer.parseInt(Port_No)+"/FS");
								pInter.invalidate(filename,originLNServer,verNum);
								break;
						}
					}
				}	
				
				// Simply broadcasting to all Super Peers about Invalidation using ALL to ALL topology
				
					for (GetTopologyDetails topo : sc.arrTD){
						if(topo.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
							List<String> neighbourArr = Arrays.asList(topo.getAll_Neighbour().split("\\s*,\\s*"));
							//System.out.println("Total Number of Neighbours in ALL TOPOLOGY : "+ neighbourArr.size());
						
						// Calling query function of Neighbors in ALL TOPOLOGY
							for (String spName : neighbourArr){
								// Getting Remote Super Peer Port Number
								for(GetSuperPeerDetails speer : sc.arrSPD){
									// Get port number of Super Peer
									if(speer.getPeer_ID().equalsIgnoreCase(spName)){
										remoteSupPeerPortNum = speer.getPeer_Port();
										break;
									}
								}
								// Calling Neighboring query method
									try{
										Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(remoteSupPeerPortNum));
										SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+remoteSupPeerPortNum);
										System.out.println("Calling Neighbour " + spName + " broadCastSS() ");
										spInter.broadCastSS(msgId, filename, originLNServer, verNum);
									}
									catch(Exception e){
										System.out.println("Exception occured at calling Neighbour Query. Neighbour is : " + spName );
									}
							}
						break;
						}
					}		
		}
		catch (Exception e1) {
			System.out.println("IOException occured while reading the property file in SuperPeer BroadcastSP function.");
		}
		
	}

	@Override
	public void broadCastSS(String msgId, String filename, String originLNServer, String verNum)
			throws RemoteException {
		//creating an object to use the config file
		SetupConfig sc;
		try {
			sc = new SetupConfig();	
			//iterate over values of config file and store the leaf IDs in an arraylist
			//array list to store leaf IDs of leaf node
			List<String> apd = new ArrayList<String>();
			for (GetSuperPeerDetails sp : sc.arrSPD){
				if(sp.getPeer_ID().equalsIgnoreCase(this.supPeerId)){
					String pd = sp.getLeaf_ID();
					apd = Arrays.asList(pd.split(","));
					break;	
				}
			}
			//extract port numbers from config file after comparing leaf IDs in array list and config file
			for(String ls: apd) {
				for(GetPeerDetails p : sc.arrPD) {
					if ( p.getPeer_ID().equalsIgnoreCase(ls)) {
						String Port_No = p.getPeer_Port();
							// create a registry to remotely access GetPeerDetails parameters.
						
							Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(Port_No));
							// Calling Leaf node interface methods
							LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+Integer.parseInt(Port_No)+"/FS");
							// call invalidate method using Leaf Node's object
							pInter.invalidate(filename,originLNServer,verNum);
							break;
					}
				}
			}
			
		}catch(Exception e) {
			//exceptions
			System.out.println("Expection occured at method broadcastSS() ");
		}
	}
	
	@Override
	public String poll(String filename, String verNum,String ogPeerId) throws RemoteException {		
		// Get Master node's registry
		Collection<ArrayList<String>> resultArrFile = searchFile(filename);
		// 0 - filename,1-peerid,2-portno,3-direct,4-superpeerid,5-copytype,6-vernum,7-status,8-lmt,9-TTR,10-ogPeerId
		String retValue="File not found at Master Node";
		
		if(!resultArrFile.isEmpty()){
			for (ArrayList<String> asr: resultArrFile){
				if(asr.get(1).equalsIgnoreCase(ogPeerId) && asr.get(5).equalsIgnoreCase("MC")){
					if(asr.get(6).equalsIgnoreCase(verNum)){
						retValue="Proper version";
					}
					else{
						retValue="File out of Date";
					}
				break;
				}
			}
		return retValue;
		}
		else{
			return retValue;
		}		
	}

	@Override
	public String getVersionNum(String filename, String peerid, String port_num, String copyType, String ogPeerId)
			throws RemoteException {
		// 0 - filename,1-peerid,2-portno,3-direct,4-superpeerid,5-copytype,6-vernum,7-status,8-lmt,9-TTR,10-ogPeerId
		Collection<ArrayList<String>> resultArrFile = new ArrayList<ArrayList<String>>();
		if(this.fileDictionary.containsKey(filename)){
			resultArrFile = this.fileDictionary.get(filename);
		}
		for (ArrayList<String> as : resultArrFile){
			if(as.get(0).equalsIgnoreCase(filename) && as.get(1).equalsIgnoreCase(peerid) && as.get(2).equalsIgnoreCase(port_num) && as.get(5).equalsIgnoreCase(copyType) && as.get(10).equalsIgnoreCase(ogPeerId)){
				return as.get(6);
			}
		}
		// Master Copy Filename entry not found under requested peer id
		return "-1";
	}

	@Override
	public void notifyPoll(String filename, String reqPeerid, String reqPort_num, String copyType, String verNum ,String ogPeerId,
			String TTR,String lastModTime) throws RemoteException, NotBoundException {

		//find master node's superpeer
		   String masterSPID = null;
		   SetupConfig scg;
			try {
				scg = new SetupConfig();
				for(GetPeerDetails gpd: scg.arrPD){
					if(gpd.getPeer_ID().equalsIgnoreCase(ogPeerId)){
						masterSPID=gpd.getSuperPeer();
						break;
					}
				}
			}
			catch (IOException e1) {
				System.out.println("IOException occured while reading the property file at Polling function Pull 1");
			}

    	// Calling Pulling method
	    	try {
				SuperPeerImpl.flagTable.put(filename, "true");
				consistentPull(masterSPID,filename,verNum,TTR,ogPeerId);
			} catch (NotBoundException e) {
				System.out.println("Not Bound Exception occured while calling consistent pull in NotifyPoll");
			}
    	
			System.out.println("Line 5");
			// Create remote object of Master leaf node
			Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(reqPort_num));
			LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+reqPort_num+"/FS");

			class MyTask implements Runnable {
		        public void run() {
		        	//System.out.println(flagTable);		//DEBUG
		            if (flagTable.get(filename).equalsIgnoreCase("false")) {
			    		// Update the cached table and registry index of its super peer   
		            	System.out.println("BHAI HO GAYA !!!!");
					    //Invalidating the IndexServer Indexes 
		        		// what is needed for upd in registry - filename,vernum,lmt,status,peerid
					    try {
							registryFiles("upd",filename, reqPeerid, "", "","","CC",verNum,"invalid",lastModTime,TTR,ogPeerId);
						} catch (RemoteException e) {
								System.out.println("Exception occured while updating the Index in NotifyPoll");
						}
						//Informing its leaf node to Invalidate its cached table.
						try {
							pInter.outOfDate(filename, "Out of Date",ogPeerId);
						} catch (RemoteException e) {
							System.out.println("Exception occured while calling Out of date function in NotifyPoll");
						}
						t.cancel(false);
		            }
		        }
		    }
			System.out.println("Line 6");
	        t = executor.scheduleAtFixedRate(new MyTask(), 0, 1, TimeUnit.SECONDS);
			System.out.println("Line 7");
		
		
	}
	
	// Leaf node 
	   public static void consistentPull(String masterPeerId,String filename,String verNum,String TTR,String ogPeerId) throws RemoteException, NotBoundException {
		   int timeToRef = Integer.parseInt(TTR);
		   // Getting port number of Master node
		   String masterPortNum = null;
		   SetupConfig scg;
			try {
				scg = new SetupConfig();
				for(GetSuperPeerDetails gspd: scg.arrSPD){
					if(gspd.getPeer_ID().equalsIgnoreCase(masterPeerId)){
						masterPortNum=gspd.getPeer_Port();
						break;
					}
				}
			}
			catch (IOException e1) {
				System.out.println("IOException occured while reading the property file at Polling function Pull 1");
			}
			
			// Create remote object of Master node's Super Peer
			Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(masterPortNum));
			SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+masterPortNum);

			//leafNodePoll is the task which needs to run repetitively 
		   final Runnable leafNodePoll = new Runnable() {
	       public void run() { 
	    	   try {
				status1Table.put(filename, spInter.poll(filename, verNum,ogPeerId));
	    	   }
	    	   catch (RemoteException e) {
					System.out.println("Remote Exception occurred while Polling the master node");
	    	   	}
	    	   System.out.println("STATUS from MASTER SUPER PEER for downloaded file "+ filename +" : " + status1Table.get(filename));
	       }
	     };
		    // Scheduling this leafNodePoll task for every TTR seconds
	     final ScheduledFuture<?> leafNodePollHandle = scheduler.scheduleAtFixedRate(leafNodePoll, 1, timeToRef, TimeUnit.SECONDS);
	     ScheduledExecutorService queueCancelCheckExecutor = Executors.newSingleThreadScheduledExecutor();

	     final Runnable stopBeep = new Runnable() {
	         public void run() {
	   	    	 if(!status1Table.isEmpty()){
		    	     if(status1Table.get(filename).equalsIgnoreCase("File out of Date")){
		    	    	 flagTable.put(filename, "false");
		    	    	 leafNodePollHandle.cancel(true); 
		    	     }	   	    		 
	   	    	 }
	         }
	       };
	       queueCancelCheckExecutor.scheduleAtFixedRate(stopBeep,1, 500, TimeUnit.MILLISECONDS);

	}

	   /*--------- end change ----------*/
	
}
