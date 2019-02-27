package com.gfiletransfer;

//import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.io.IOException;
//import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SetupConfig {

	//initialize all array lists that need to be loaded.
	
	ArrayList<GetPeerDetails> arrPD = new ArrayList<GetPeerDetails>();
	ArrayList<GetSuperPeerDetails> arrSPD = new ArrayList<GetSuperPeerDetails>();
	ArrayList<GetTopologyDetails> arrTD = new ArrayList<GetTopologyDetails>();
	String topology= null;
	/*--------- start change ----------*/
	String consisApp=null;
	/*--------- end change ----------*/
	
	//ArrayList<ReadConfig> PeerList = new ArrayList<ReadConfig>();
	public SetupConfig() throws IOException{
		
		//Peer Properties: Peer_ID,Peer_Port,MasterDir1,CacheDir2,SuperPeer
		File fpc = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//AOS//Homework-Programming//PA-3//peer_config.txt");
		//File fpc = new File("/Users/vjstark/Documents/Consistency/peer_config.txt");
		
		List<String> arrfpc = new ArrayList<String>();
		ArrayList<List<String>> finalArrP = new ArrayList<List<String>>();
		Scanner sc1 = new Scanner(fpc);
		
		while((sc1.hasNextLine())) {
			String peerinfo = sc1.nextLine();
			
			if(peerinfo.length() != 0 && !peerinfo.startsWith("*Peer") && !peerinfo.startsWith("#") ) {
				arrfpc = Arrays.asList(peerinfo.split("\\s*,\\s*"));
				finalArrP.add(arrfpc);
			}			
		}
		sc1.close();
		for(List<String> ls : finalArrP) {
			GetPeerDetails pd = new GetPeerDetails();
			pd.setPeer_ID(ls.get(0));
			pd.setPeer_Port(ls.get(1));
			pd.setDir(ls.get(2));
			pd.setCacheDir(ls.get(3));
			pd.setSuperPeer(ls.get(4));
			arrPD.add(pd);
		}
		
		//SuperPeer Properties: Peer_ID;Peer_Port;Leaf_IDs
		File fspc = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//AOS//Homework-Programming//PA-3//superpeer_config.txt");
		//File fspc = new File("/Users/vjstark/Documents/Consistency/superpeer_config.txt");

		
		List<String> arrfspc = new ArrayList<String>();
		ArrayList<List<String>> finalArrSP = new ArrayList<List<String>>();
		Scanner sc2 = new Scanner(fspc);
		
		while((sc2.hasNextLine())) {
			String speerinfo = sc2.nextLine();
			
			if(speerinfo.length() != 0 && !speerinfo.startsWith("*SuperPeer") && !speerinfo.startsWith("#")) {
				arrfspc = Arrays.asList(speerinfo.split("\\s*;\\s*"));
				//System.out.println(arrfspc);
				finalArrSP.add(arrfspc);
			}
		}
		sc2.close();
		for(List<String> ls: finalArrSP) {
			GetSuperPeerDetails spd = new GetSuperPeerDetails();
			spd.setPeer_ID(ls.get(0));
			spd.setPeer_Port(ls.get(1));
			spd.setLeaf_ID(ls.get(2));
			arrSPD.add(spd);
		}
		
		//All_To_All_Topology and Linear Topology Properties : Peer_ID;All_Neighbours;Linear_Neigbour
		File ft = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//AOS//Homework-Programming//PA-3//TopologyDetails_config.txt");
		//File ft = new File("/Users/vjstark/Documents/Consistency/TopologyDetails_config.txt");
		
		List<String> arrftd = new ArrayList<String>();
		ArrayList<List<String>> finalArrTD = new ArrayList<List<String>>();
		Scanner sc3 = new Scanner(ft);
		
		while((sc3.hasNextLine())) {
			String tdinfo = sc3.nextLine();
			
			if(tdinfo.length() != 0 && !tdinfo.startsWith("*Topology") && !tdinfo.startsWith("#")) {
				arrftd = Arrays.asList(tdinfo.split("\\s*;\\s*"));
				//System.out.println(arrftd);
				finalArrTD.add(arrftd);
			}
		}
		sc3.close();
		for(List<String> ls: finalArrTD) {
			GetTopologyDetails td = new GetTopologyDetails();
			td.setPeer_ID(ls.get(0));
			td.setLinear_Neighbour(ls.get(1));
			td.setAll_Neighbour(ls.get(2));
			arrTD.add(td);
		}
	
// get topology	
		File topfc = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//AOS//Homework-Programming//PA-3//Topology.txt");
		//File topfc = new File("/Users/vjstark/Documents/Consistency/Topology.txt");
		
		String topoInfo=null;
		Scanner sc5 = new Scanner(topfc);
		
		while((sc5.hasNextLine())) {
			String speerinfo = sc5.nextLine();	
			if(speerinfo.length() != 0 && !speerinfo.startsWith("*Topology") && !speerinfo.startsWith("#")) {
				topoInfo = speerinfo;
				//System.out.println(topoInfo);
			}
		}
		sc5.close();
		topology=topoInfo;
		
		/*--------- start change ----------*/
		// get topology	
				File confc = new File("K://IIT Life//Lecture Notes//SEM 1 - DIC AOS DPA//AOS//Homework-Programming//PA-3//Consistency_config.txt");
				//File confc = new File("/Users/vjstark/Documents/Consistency/Consistency_config.txt");
				String consisInfo=null;
				Scanner sc6 = new Scanner(confc);
				
				while((sc6.hasNextLine())) {
					String consistentinfo = sc6.nextLine();
					if(consistentinfo.length() != 0 && !consistentinfo.startsWith("*Consistency") && !consistentinfo.startsWith("#")) {
						consisInfo = consistentinfo;
						//System.out.println(consisInfo);
					}
				}
				sc6.close();
				consisApp=consisInfo;
		/*--------- end change ----------*/
	}
}
