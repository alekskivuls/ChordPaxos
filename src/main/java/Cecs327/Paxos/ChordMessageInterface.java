package Cecs327.Paxos;
import java.io.InputStream;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ChordMessageInterface extends Remote {
	public ChordMessageInterface getPredecessor() throws RemoteException;
	
	public ChordMessageInterface getSuccessor() throws RemoteException;

	ChordMessageInterface locateSuccessor(long key) throws RemoteException;

	ChordMessageInterface closestPrecedingNode(long key) throws RemoteException;

	public void joinRing(String Ip, int port) throws RemoteException;

	public void notify(ChordMessageInterface j) throws RemoteException;

	public boolean isAlive() throws RemoteException;

	public long getId() throws RemoteException;

	public void put(long guidObject, InputStream inputStream) throws RemoteException;

	public InputStream get(long guidObject) throws RemoteException;

	public void delete(long guidObject) throws RemoteException;
}
