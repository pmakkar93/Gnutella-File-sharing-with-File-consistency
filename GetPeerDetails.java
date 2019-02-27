package com.gfiletransfer;

public class GetPeerDetails {

	private String Peer_ID = null;
	private String Peer_Port = null;
	private String Dir = null;
	private String CacheDir = null;
	private String SuperPeer = null;
	
	
	public String getPeer_ID() {
		return Peer_ID;
	}
	
	public void setPeer_ID(String peer_ID) {
		Peer_ID = peer_ID;
	}
	
	public String getPeer_Port() {
		return Peer_Port;
	}
	
	public void setPeer_Port(String peer_Port) {
		Peer_Port = peer_Port;
	}
	
	public String getDir() {
		return Dir;
	}
	
	public void setDir(String dir) {
		Dir = dir;
	}
	
	public String getSuperPeer() {
		return SuperPeer;
	}
	
	public void setSuperPeer(String superPeer) {
		SuperPeer = superPeer;
	}

	public String getCacheDir() {
		return CacheDir;
	}

	public void setCacheDir(String cacheDir) {
		CacheDir = cacheDir;
	}
	
	
}

/*
 * Peer_ID = P01
 * Peer_Port = 4001
 * Dir = "abcd"
 * SuperPeer = SP01
 */