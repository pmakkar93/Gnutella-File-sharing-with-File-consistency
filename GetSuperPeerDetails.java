package com.gfiletransfer;

public class GetSuperPeerDetails {

	private String Peer_ID = null;
	private String Peer_Port = null;
	private String Leaf_ID = null;
	
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

	public String getLeaf_ID() {
		return Leaf_ID;
	}

	public void setLeaf_ID(String leaf_ID) {
		Leaf_ID = leaf_ID;
	}
}

