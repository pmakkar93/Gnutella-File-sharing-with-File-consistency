package com.gfiletransfer;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;

public interface LeafNodeInterface extends Remote{
	public byte[] fileDownload(ArrayList<String> searchedDir) throws RemoteException;
	public boolean queryHit(String msgId, int TTL, String filename,Collection<ArrayList<String>> resultArr) throws RemoteException;
	/*--------- start change ----------*/
	public String poll(String filename,String verNum) throws RemoteException,NotBoundException;
	public void invalidate(String filename,String originLNServer,String verNum) throws RemoteException;
	public void outOfDate(String filename, String invalidStatus,String ogPeerId) throws RemoteException;
	public String getStatus(String filename) throws RemoteException;
	/*--------- end change ----------*/
}
