package com.gfiletransfer;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;

public interface SuperPeerInterface extends Remote{
	public void registryFiles(String rd,String filename, String peerid, String port_num, String directory,String sPeer,String copyType,String versionNum,String status,String lastModTime,String TTR, String ogPeerId) throws RemoteException;
	public Collection<ArrayList<String>> searchFile(String filename) throws RemoteException;
	public void query(String msgId, int TTL, String filename,String reqPeerId,String reqPortNum) throws RemoteException;
	public void queryHit(String msgId, int TTL, String filename,Collection<ArrayList<String>> resultArr) throws RemoteException;
	/*--------- start change ----------*/
	public void broadCastSP(String msgId, String filename,String originLNServer,String verNum) throws RemoteException;
	public void broadCastSS(String msgId, String filename,String originLNServer,String verNum) throws RemoteException;
	public String poll(String filename,String verNum,String ogPeerId) throws RemoteException;
	public String getVersionNum(String filename,String peerid, String port_num,String copyType,String ogPeerId) throws RemoteException;
	public void notifyPoll(String filename,String reqPeerid, String reqPort_num,String copyType,String verNum,String ogPeerId,String TTR,String lastModTime) throws RemoteException, NotBoundException;
	/*--------- end change ----------*/
}
