package com.gfiletransfer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import java.util.Timer;
import java.util.Map.Entry;

public class LeafNodeImpl implements LeafNodeInterface {

	String portNo = null; // Port no. of the peer
	String dirName = null; //Directory where master files are to be stored.
	String cachedDirName = null; //Directory where downloaded / cache files are to be stored.	
	String fileName = null; //the file to be searched.
	String remotePeer= null; //Peer from whom file has to be downloaded.
	String superpeer = null; // name of super peer or id
	String peerID = null; //peerID (fetch and set it from property file)
	int seqNum = -1;
	int broadSeqNum=-1;
	int timeTL = 20; // 3 TTL for All to All Topology and 22 TTL for Linear Topology
	String msgId = null;
	String consistencyType=null;
	String verString=null;
	/*--------- start change ----------*/
	final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(15); // no
	static ScheduledFuture<?> t;
	public static  Map<String,String> flagTable = new HashMap<String,String>();
	public static  Map<String,String> status1Table = new HashMap<String,String>();
	static String status1 = null;
	
	
	private MultivaluedMap<String, ArrayList<String>> finalHM = new MultivaluedHashMap<>();
	private Map<String,ArrayList<String>> cachedTable = new HashMap<String,ArrayList<String>>();
	/*--------- end change ----------*/
	
	LeafNodeImpl(String portNo, String dirName, String superpeer, String peerID, String cachedDirName){
		this.portNo = portNo;
		this.dirName = dirName;
		this.superpeer = superpeer;
		this.peerID = peerID;
		this.cachedDirName = cachedDirName;
	}
	// ###########################
	
	public void doWork() throws IOException {
		String superPeerPort = null;
		// Reading Super Peer Port details from property file for connecting to Indexing server(SP)
	    SetupConfig scg;
		try {
			scg = new SetupConfig();
			// Getting Calling Super Peer Port number
			for (GetSuperPeerDetails sp : scg.arrSPD){
				if(sp.getPeer_ID().equalsIgnoreCase(this.superpeer)){
					superPeerPort = sp.getPeer_Port();
					break;
				}
			}
			consistencyType=scg.consisApp;
		}
		catch (IOException e1) {
			System.out.println("IOException occured while reading the property file at connecting to Super Peer.");
		}
		
		try{
			// Locating Registry of Indexing Server and obtains target address 
			Registry regis = LocateRegistry.getRegistry("localhost", Integer.parseInt(superPeerPort));
			SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+superPeerPort);
			Scanner sc = new Scanner(System.in);
//			System.out.println("Enter Your Peer ID");
//			peerID = sc.nextLine();
			
			//obtain directory name where file is located
			File dirList = new File(dirName);
			//list of all records in the directory
			String[] record = dirList.list();

			// creating a version of the master copy file
			int versionNumEdit=1;
			verString="v"+String.format("%02d", versionNumEdit);
			//System.out.println(verString);

			// Registering Files in Index Server
			for(int c=0; c < record.length; c++){
				File currentFile = new File(record[c]);
				System.out.println("Registering details of File name " + currentFile.getName() + " in Indexing Server");
				/*--------- start change ----------*/
				String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
				spInter.registryFiles("new",currentFile.getName(), peerID, portNo, dirName,superpeer,"MC",verString,"valid",timeStamp,"30",peerID);
				/*--------- end change ----------*/
			}
			
			System.out.println("Do you want to Search a File, Delete File, Edit file or Exit? (Search/Delete/Edit/Exit)");
			String sd= sc.nextLine();
			while(!sd.equalsIgnoreCase("Exit")){
				if(sd.equalsIgnoreCase("Delete")){
					//Deleting a File from local peer's Directory
					String wantToDel="";
					while(!wantToDel.equalsIgnoreCase("No")){
						System.out.println("Enter the file name which you want to delete");
						String fname = sc.nextLine();
						if(fname!=null){
							File fileToDel = new File(dirName+"\\"+fname);
							// Delete the specified file from local peer's directory
							if(fileToDel.delete()){
								System.out.println("File deleted Successfully.");
								// Updating the index server about the deleted file
								spInter.registryFiles("del",fname, peerID, portNo, dirName,superpeer,"","","","","",peerID);
							}
							else{
								System.out.println("Failed to delete the File");
							}
						}
						else{
							System.out.println("Please Enter a Filename");
						}
						System.out.println("Do you want to delete more files? (Yes/No)");
						wantToDel=sc.nextLine();
					}
				}
				/*--------- start change ----------*/
				//EDIT PART START
				if(sd.equalsIgnoreCase("Edit")){
					//Editing a File from local peer's Directory
					String wantToEdit="";
					while(!wantToEdit.equalsIgnoreCase("No")){
						System.out.println("Enter the file name which you want to Edit(Append)");
						String fname = sc.nextLine();
						if(fname!=null){
							File fileToEdit = new File(dirName+"\\"+fname);
							
							System.out.println("Enter anything you want to append in this file");
							String appendString = sc.nextLine();
							
							FileWriter fw = new FileWriter(dirName+"\\"+fname,true);
							fw.write(appendString);
							fw.close();
							System.out.println("File edited Successfully.");

							// get recent version number
							verString = spInter.getVersionNum(fname,peerID, portNo, "MC",peerID);

							if(verString.equalsIgnoreCase("-1")){
								System.out.println("File not found under Peer "+ peerID + " in Registry Index");
							}
							// proper version is fetched 
							else{
								// Updating the version of edited file
								int intvernum = Integer.parseInt(verString.substring(1));	// Fetch the current version of file from RegistryIndex 
								intvernum +=1;
								verString="v"+String.format("%02d", intvernum);
								System.out.println("New Version Number for edited file : "+verString);
								
								try{
									String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
									spInter.registryFiles("upd",fname, peerID, portNo, dirName,superpeer,"MC",verString,"valid",timeStamp,"",peerID);									
								}catch(Exception e){
									System.out.println("Exception occurred: Updating the Register Index at Super Peer");
								}
								// Updating the index server about the edited file
								// Send invalidate request to all nodes (PUSH)
								if(consistencyType.equalsIgnoreCase("PUSH")){
									System.out.println("Send invalidate request to all nodes (PUSH)");
									broadSeqNum=broadSeqNum+1;
									String messId=peerID+":"+superpeer+ ":" + String.format("%02d", broadSeqNum);
									spInter.broadCastSP(messId,fname,peerID,verString);
								}
							}							
						}
						else{
							System.out.println("Please Enter a Filename");
						}
						System.out.println("Do you want to edit more files? (Yes/No)");
						wantToEdit=sc.nextLine();
					}
				}
				// EDIT PART END
				/*--------- end change ----------*/
				else if(sd.equalsIgnoreCase("Search")){
					//Searching and downloading a File Code
					String ans= "";

					while(!ans.equalsIgnoreCase("No")){
						// Searching the file in Indexing server
						seqNum=seqNum+1;
						System.out.println("Enter the file name which you want to search");
						fileName = sc.nextLine();
						if(fileName!=null){
							msgId = peerID + ":" + Integer.toString(seqNum);	
							
//							spInter.query(msgId, timeTL, fileName, peerID, portNo);
							
						// Adding a timer for 5 ms of searching
							ExecutorService service = Executors.newSingleThreadExecutor();
							try {
							    Runnable r = new Runnable() {
							        @Override
							        public void run() {
							        	try {
							        		System.out.println("Now Started Calling the query() from Leaf Node...");
												spInter.query(msgId, timeTL, fileName, peerID, portNo);
							        		//System.out.println("Still running the run method");
										} catch (RemoteException e) {
											// TODO Auto-generated catch block
											System.out.println("TimeOut : It ran too long. Need to stop searching and continue.");
										}
							        }
							    };
							    Future<?> f = service.submit(r);
							    f.get(4, TimeUnit.SECONDS);     // attempt the search for 5 seconds
							}
							catch (final InterruptedException e) {
							    // The thread was interrupted during sleep, wait or join								
								System.out.println("Interrupted Exception Occured");
							}
							catch (final TimeoutException e) {
							    System.out.println("TimeOut Exception Occured. It ran too long. Need to stop searching and continue.");
							}
							catch (final ExecutionException e) {
							    // An exception from within the Runnable task
								System.out.println("Execution Exception Occured");
							}
							finally {
							    service.shutdownNow();
							}
													
						}
						else{
							System.out.println("Please Enter a Filename");
						}		
						
						Collection<ArrayList<String>> finalRes = this.finalHM.get(msgId);
						String refVerNum = null;
						/*--------- start change ----------*/
						
						if(!finalRes.isEmpty()){
							// Taking Version Number of Master Copy to check validity of Cached copies
							for(ArrayList<String> as : finalRes){
								if(as.get(0).equalsIgnoreCase(fileName)){
									if(as.get(5).equalsIgnoreCase("MC")){
										refVerNum = as.get(6);
										break;
									}	
								}
							}
							// Displaying Peers List which can provide the requested file
							System.out.println("#################################################");
							System.out.println("\n");	
							for(ArrayList<String> als : finalRes){
								if(als.get(0).equalsIgnoreCase(fileName)){									
									if(als.get(5).equalsIgnoreCase("MC")){
										System.out.println("VALID Peer providing the file with Peer ID is "+ als.get(1)+ " under Super Peer :" + als.get(4) + " which is a Master Copy");	
									}
									else{
								// Validation check using version number of cached copy and master copy
										if(als.get(6).equalsIgnoreCase(refVerNum)){
											System.out.println("VALID Peer providing the file with Peer ID is "+ als.get(1)+ " under Super Peer :" + als.get(4) + " which is a Cached Copy");
										}
									}
								}
							}
							System.out.println("\n");
							System.out.println("#################################################");
							/*--------- end change ----------*/
							// Choosing one of the returned Peer
							System.out.println("Enter Peer ID you wish to take the file from");
							remotePeer = sc.nextLine();
							
							// Downloading the file from Specified Peer
							int co=finalRes.size();
							if(remotePeer!=null){
								for(ArrayList<String> als : finalRes){
									if(als.get(1).equalsIgnoreCase(remotePeer)){
										// Looking up from the Registry for Selected Peer
										Registry regis2 = LocateRegistry.getRegistry("localhost", Integer.parseInt(als.get(2)));
										LeafNodeInterface lnInter = (LeafNodeInterface) regis2.lookup("root://LeafNode/"+als.get(2)+"/FS");	
										
										/*--------- start change ----------*/
										// if choosen node is Master copy node, then direct download
										if(als.get(5).equalsIgnoreCase("MC")){
											byte[] output= lnInter.fileDownload(als);
											System.out.println(output.length);
											
											// Converting Downloaded byte array into file
											if(output.length!=0){
												FileOutputStream ostream = null;
												try {
													ostream = new FileOutputStream(cachedDirName+"\\"+fileName);
												    ostream.write(output);
												    System.out.println("File Downloading Successful.");
												    System.out.println("Display File " + fileName);
												    
												    //Updating the IndexServer Indexes after downloading the file.
												    spInter.registryFiles("new",fileName, peerID, portNo, cachedDirName,superpeer,"CC",als.get(6),"valid",als.get(8),als.get(9),als.get(10)); 
												    
												    //Adding downloaded details in Cached Table
												    // 0- filename,1-copy_type,2-cachedDirectory,3-versionNumber,4-OriginServerId,5-status,6-TTR
												    ArrayList<String> ct = new ArrayList<String>();
												    ct.add(fileName); // filename
												    ct.add("CC");	//copy_type
												    ct.add(cachedDirName); //cachedDirectory
												    ct.add(als.get(6)); 	//versionNumber
												    ct.add(als.get(10));	//OriginServerId
												    ct.add("valid");		//status
												    ct.add(als.get(9));		// TTR
												    this.cachedTable.put(fileName, ct);
													    // Displaying all the Cached table details 
														System.out.println("Updated Cached Table Entry after insertion (File download)");
														for (Entry<String, ArrayList<String>> entry : this.cachedTable.entrySet()) {
														    System.out.println(entry.getKey() + " => " + entry.getValue());
														}
																		    
												    // Polling implementation starts here
												    if(consistencyType.equalsIgnoreCase("PULL1")){												    	
														LeafNodeImpl.flagTable.put(fileName, "true");
												    	// Calling Pulling method
												    	consistentPull(als.get(10),fileName,als.get(6),als.get(9));
												    	
														System.out.println("Line 5");													
														class MyTask implements Runnable {
													        public void run() {
													        	//System.out.println(flagTable);		//DEBUG
													            if (flagTable.get(fileName).equalsIgnoreCase("false")) {
														    		// Update the cached table and registry index of its super peer   
													            	System.out.println("BHAI HO GAYA !!!!");
																    //Invalidating the IndexServer Indexes after downloading the file.
																    try {
																		spInter.registryFiles("upd",fileName, peerID, portNo, cachedDirName,superpeer,"CC",als.get(6),"invalid",als.get(8),als.get(9),als.get(10));
																	} catch (RemoteException e) {
																			System.out.println("Exception occured while updating the Index");
																	}
																  //Invalidating the cached table after downloading the file.
																		lnInvalidate(fileName, als.get(10));
														                t.cancel(false);
													            }
													            else{
													            //	System.out.println("DEBUG OF ELSE"); //DEBUG
													            }
													        }
													    }
														System.out.println("Line 6");
												        t = executor.scheduleAtFixedRate(new MyTask(), 0, 1, TimeUnit.SECONDS);
														System.out.println("Line 7");
												    }
												    else if(consistencyType.equalsIgnoreCase("PULL2")){
													    // 0- filename,1-copy_type,2-cachedDirectory,3-versionNumber,4-OriginServerId,5-status,6-TTR
												    	if(!this.cachedTable.isEmpty()){
												    		if(this.cachedTable.containsKey(fileName)){
															    ArrayList<String> ct1 = this.cachedTable.get(fileName);
															    spInter.notifyPoll(fileName, peerID, portNo, ct1.get(1), ct1.get(3), ct1.get(4), ct1.get(6),als.get(8));
												    		}
												    	}												    	
												    }
												}
												catch(Exception e){
													System.out.println("Exception in bytearray to file conversion. " + e.getMessage());
												}
												finally {
												    ostream.close();
												}											
											}
											else{
												System.out.println("File is not present at Remote Location.");
											}
											break;
										}
										// If choosen node is cached copy, then again revalidating the selected node and later download the file
										else{
											if(lnInter.getStatus(als.get(0)).equalsIgnoreCase("valid")){
												// Calling Remote File Download method of Selected Peer
												byte[] output= lnInter.fileDownload(als);
												System.out.println(output.length);
												
												// Converting Downloaded byte array into file
												if(output.length!=0){
													FileOutputStream ostream = null;
													try {
														ostream = new FileOutputStream(cachedDirName+"\\"+fileName);
													    ostream.write(output);
													    System.out.println("File Downloading Successful.");
													    System.out.println("Display File " + fileName);
													    
													    //Updating the IndexServer Indexes after downloading the file.
													    spInter.registryFiles("new",fileName, peerID, portNo, cachedDirName,superpeer,"CC",als.get(6),"valid",als.get(8),als.get(9),als.get(10));
													    
													    //Adding downloaded details in Cached Table
													    ArrayList<String> ct = new ArrayList<String>();
													    ct.add(fileName);
													    ct.add("CC");
													    ct.add(cachedDirName);
													    ct.add(als.get(6));
													    ct.add(als.get(10));
													    ct.add("valid");
													    ct.add(als.get(9));
													    this.cachedTable.put(fileName, ct);
													    // Displaying all the Cached table details 
														System.out.println("Updated Cached Table Entry after insertion (File download)");
														for (Entry<String, ArrayList<String>> entry : this.cachedTable.entrySet()) {
														    System.out.println(entry.getKey() + " => " + entry.getValue());
														}
														
														
													    // Polling implementation starts here
													    if(consistencyType.equalsIgnoreCase("PULL1")){												    	
															this.flagTable.put(fileName, "true");
													    	// Calling Pulling method
													    	consistentPull(als.get(10),fileName,als.get(6),als.get(9));

															System.out.println("Line 5");													
															class MyTask implements Runnable {
														        public void run() {
														        	//System.out.println(flagTable);  /////////////// DEBUG
														            if (flagTable.get(fileName).equalsIgnoreCase("false")) {
															    		// Update the cached table and registry index of its super peer   
														            	System.out.println("BHAI HO GAYA !!!!");
																	    //Invalidating the IndexServer Indexes after downloading the file.
																	    try {
																			spInter.registryFiles("upd",fileName, peerID, portNo, cachedDirName,superpeer,"CC",als.get(6),"invalid",als.get(8),als.get(9),als.get(10));
																		} catch (RemoteException e) {
																				System.out.println("Exception occured while updating the Index");
																		}
																	  //Invalidating the cached table after downloading the file.
																			lnInvalidate(fileName, als.get(10));
															                t.cancel(false);
														            }
														        }
														    }
															System.out.println("Line 6");
													        t = executor.scheduleAtFixedRate(new MyTask(), 0, 1, TimeUnit.SECONDS);
															System.out.println("Line 7");
													    }
													    else if(consistencyType.equalsIgnoreCase("PULL2")){
														    // 0- filename,1-copy_type,2-cachedDirectory,3-versionNumber,4-OriginServerId,5-status,6-TTR
													    	if(!this.cachedTable.isEmpty()){
													    		if(this.cachedTable.containsKey(fileName)){
																    ArrayList<String> ct1 = this.cachedTable.get(fileName);
																    spInter.notifyPoll(fileName, peerID, portNo, ct1.get(1), ct1.get(3), ct1.get(4), ct1.get(6),als.get(8));
													    		}
													    	}												    	
													    }

													}
													catch(Exception e){
														System.out.println("Exception in bytearray to file conversion. " + e.getMessage());
													}
													finally {
													    ostream.close();
													}											
												}
												/*--------- end change ----------*/
												else{
													System.out.println("File is not present at Remote Location.");
												}
												break;
											}
											else{
												System.out.println("The Peer which you had selected, just got its file invalidated. Please select Master Copy for guaranteed file download.");
											}
										} 
									}
									else{
										if(co==1)
											System.out.println("Peer with that ID " + remotePeer + " does not exist. Please choose proper PeerId.");
									}
								co--;
								}
							}
							else{
								System.out.println("Please enter proper Peer ID");
							}
						}
						else{
							System.out.println("Sorry, File which you are searching doesnt exist in our Server.");
						}
						System.out.println("Do you want to search again ? (Yes/No)");
						ans=sc.nextLine();
					}				
				}
				else{
					System.out.println("Please select appropriate choice");
				}				
				System.out.println("Do you want to Search a File, Delete File or Exit? (Search/Delete/Exit)");
				sd= sc.nextLine();
			}
			System.exit(0);
		}catch(Exception e) {
			System.out.println("Exception at Client Interface: " + e.getMessage());
		}
	}

	@Override
	public byte[] fileDownload(ArrayList<String> searchedDir) throws RemoteException{
		//0 filename, 1 peerid, 2 port_num, 3 direct, 4 superpeer id
		String fname = searchedDir.get(0);
		String remoteDir=searchedDir.get(3);
		try {
	         File file = new File(remoteDir+"\\"+fname);
//	         System.out.println(file.exists());
	         if(file.exists()){
	     		byte buffer[] = Files.readAllBytes(file.toPath());
		         return buffer;	        	 
	         }
	      }
		catch(Exception e){
	         System.out.println("Error in File download part " + e.getMessage());
	         e.printStackTrace();
	         return new byte[0];
	      }
		return new byte[0];
	}
	@Override
	public synchronized boolean queryHit(String msgId, int TTL, String filename, Collection<ArrayList<String>> resultArr)
			throws RemoteException {
		if(TTL>0 && TTL != 0){
			try{
				/*--------- start change ----------*/
				for(ArrayList<String> arrFileDtl : resultArr){
					this.finalHM.add(msgId, arrFileDtl);	
				}
				/*--------- end change ----------*/
				return true;
			}catch(Exception e){
					System.out.println("Exception at Peer's Interface " + e.getMessage());
					return false;
				}
		}
		else{
			System.out.println("Time to Live of a Message has expired at Leaf Node. This Message is no longer valid.");
			return false;
		}
	}

	/*--------- start change ----------*/
	@Override
	public String poll(String filename, String verNum) throws RemoteException, NotBoundException {
		// Reading Master Node's Super Peer Port number
		String spPort = null;
	    SetupConfig scg;
		try {
			scg = new SetupConfig();
			// Getting Calling Super Peer Port number
			for (GetSuperPeerDetails sp : scg.arrSPD){
				if(sp.getPeer_ID().equalsIgnoreCase(this.superpeer)){
					spPort = sp.getPeer_Port();
					break;
				}
			}
		}
		catch (IOException e1) {
			System.out.println("IOException occured while reading the property file in Polling function");
		}
		
		// Creating remote object of Master Node's Super Peer 
		Registry regis = LocateRegistry.getRegistry("localhost", Integer.parseInt(spPort));
		SuperPeerInterface spInter = (SuperPeerInterface) regis.lookup("root://SuperPeer/"+spPort);
		
		// Get Master node's registry
		Collection<ArrayList<String>> resultArrFile = spInter.searchFile(filename);
		// 0 - filename,1-peerid,2-portno,3-direct,4-superpeerid,5-copytype,6-vernum,7-status,8-lmt,9-TTR,10-ogPeerId
		String retValue="File not found at Master Node";
		
		if(!resultArrFile.isEmpty()){
			for (ArrayList<String> asr: resultArrFile){
				if(asr.get(1).equalsIgnoreCase(this.peerID) && asr.get(5).equalsIgnoreCase("MC")){
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
	//finame5
	public void invalidate(String filename,String originLNserver, String verNum) throws RemoteException {
		// TODO Auto-generated method stub
		//check if filename is present in cached table
		if (this.cachedTable.containsKey(filename)) {
			// if found iterate over all keys and get version Number from values
			for( String key : this.cachedTable.keySet()) {
				if( key.equalsIgnoreCase(filename)) {
					ArrayList<String> ls = this.cachedTable.get(key);
					// compare version numbers. If they do not match Invalidate the file
					if(ls.get(4).equalsIgnoreCase(originLNserver) && !(ls.get(3).equalsIgnoreCase(verNum))) {
						ls.set(5, "invalid");
						this.cachedTable.remove(key);
						this.cachedTable.put(key, ls);
					}// if version numbers matches do nothing
					break;
				}
			}					
		    // Displaying all the Cached table details 
			System.out.println("Updated Cached Table Entry after updation");
			for (Entry<String, ArrayList<String>> entry : this.cachedTable.entrySet()) {
			    System.out.println(entry.getKey() + " => " + entry.getValue());
			}		
		}
		else {
			//filename not found
			System.out.println("Entry not found in cached table");
		}
	}

	@Override
	public void outOfDate(String filename, String invalidStatus,String ogPeerId) throws RemoteException {
		if(invalidStatus.equalsIgnoreCase("Out of Date")){
			System.out.println("Received notification from Super Peer to invalidate the file." + filename);
			lnInvalidate(filename,ogPeerId);
		}
	}

	@Override
	public String getStatus(String filename) throws RemoteException {
		// TODO Auto-generated method stub
		if (this.cachedTable.containsKey(filename)) {
			for( String key : this.cachedTable.keySet()) {
				if( key.equalsIgnoreCase(filename)) {
					ArrayList<String> ls = this.cachedTable.get(key);
					return ls.get(5);
				}
			}
		}
		return "-1";
	}
	
	public void lnInvalidate(String filename,String originLNserver){
		if (this.cachedTable.containsKey(filename)) {
			// if found iterate over all keys and get version Number from values
			for( String key : this.cachedTable.keySet()) {
				if( key.equalsIgnoreCase(filename)) {
					ArrayList<String> ls = this.cachedTable.get(key);
					// compare version numbers. If they do not match Invalidate the file
					if(ls.get(4).equalsIgnoreCase(originLNserver)) {
						ls.set(5, "invalid");
						this.cachedTable.remove(key);
						this.cachedTable.put(key, ls);
					}// if version numbers matches do nothing
					break;
				}
			}					
		    // Displaying all the Cached table details 
			System.out.println("Updated Cached Table Entry after Invalidation from Pull");
			for (Entry<String, ArrayList<String>> entry : this.cachedTable.entrySet()) {
			    System.out.println(entry.getKey() + " => " + entry.getValue());
			}		
		}
		else {
			System.out.println("Entry not found in cached table");
		}
	}
	   public static void consistentPull(String masterPeerId,String filename,String verNum,String TTR) throws RemoteException, NotBoundException {

		   int timeToRef = Integer.parseInt(TTR);
		   // Getting port number of Master node
		   String masterPortNum = null;
		   SetupConfig scg;
			try {
				scg = new SetupConfig();
				for(GetPeerDetails gpd: scg.arrPD){
					if(gpd.getPeer_ID().equalsIgnoreCase(masterPeerId)){
						masterPortNum=gpd.getPeer_Port();
						break;
					}
				}
			}
			catch (IOException e1) {
				System.out.println("IOException occured while reading the property file at Polling function Pull 1");
			}
			
			// Create remote object of Master leaf node
			Registry regis = LocateRegistry.getRegistry("localhost",Integer.parseInt(masterPortNum));
			LeafNodeInterface pInter = (LeafNodeInterface) regis.lookup("root://LeafNode/"+masterPortNum+"/FS");

			//leafNodePoll is the task which needs to run repetitively 
		   final Runnable leafNodePoll = new Runnable() {
	       public void run() { 
	    	   try {
//				status1 = ;
				status1Table.put(filename, pInter.poll(filename, verNum));
	    	   }
	    	   catch (RemoteException e) {
					System.out.println("Remote Exception occurred while Polling the master node");
	    	   	} catch (NotBoundException e) {
					System.out.println("Not Bound Exception occurred while Polling the master node");			
				}
	    	   System.out.println("STATUS from MASTER NODE for downloaded file "+ filename +" : " + status1Table.get(filename));
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
