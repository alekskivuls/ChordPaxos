package Cecs327.Paxos;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Timer;
import java.util.TimerTask;

public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface {
	public static final int M = 2;
	public static final int STABILIZE_TIMER = 500;

	Registry registry; // rmi registry for lookup the remote objects.
	ChordMessageInterface predecessor;
	ChordMessageInterface[] fingers; // Successor is finger 0
	long guid;

	public Chord(int port, long guid) throws RemoteException {
		this.guid = guid;
		fingers = new ChordMessageInterface[M];
		for (int i = 0; i < M; i++) {
			fingers[i] = this;
		}

		predecessor = this;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				stabilize();
				fixFingers();
			}
		}, STABILIZE_TIMER, STABILIZE_TIMER);
		try {
			registry = LocateRegistry.createRegistry(port);
			registry.rebind("Chord", this);
		} catch (RemoteException e) {
			throw e;
		}
	}

	public void joinRing(String ip, int port) throws RemoteException {
		try {
			Registry registry = LocateRegistry.getRegistry(ip, port);
			ChordMessageInterface chord = (ChordMessageInterface) (registry.lookup("Chord"));
			fingers[0] = chord.locateSuccessor(this.getId());
			fingers[0].notify(this);
			//predecessor = fingers[0].getPredecessor();
			
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	public ChordMessageInterface locateSuccessor(long key) throws RemoteException {
		if (key == guid)
			throw new IllegalArgumentException("Key must be distinct that  " + guid);
		// Check if this node is the only node in the ring
		if (fingers[0].getId() != this.getId()) {
			if (isKeyInSemiCloseInterval(key, guid, fingers[0].getId()))
				return fingers[0];
			return closestPrecedingNode(key).locateSuccessor(key);
		}
		return fingers[0];
	}

	public void put(long guidObject, InputStream stream) throws RemoteException {
		try {
			String fileName = "./" + guid + "/repository/" + guidObject;
			FileOutputStream output = new FileOutputStream(fileName);
			while (stream.available() > 0)
				output.write(stream.read());
			output.close();
		} catch (IOException e) {
			throw (new RemoteException(e.getMessage()));
		}
	}

	public InputStream get(long guidObject) throws RemoteException {
		InputStream file = null;
		try {
			String fileName = "./" + guid + "/repository/" + guidObject;
			file = new FileStream(fileName);
		} catch (IOException e) {
			throw (new RemoteException(e.getMessage()));
		}
		return file;
	}

	public void delete(long guidObject) throws RemoteException {
		String fileName = "./" + guid + "/repository/" + guidObject;
		File file = new File(fileName);
		file.delete();
	}

	public long getId() throws RemoteException {
		return guid;
	}

	public boolean isAlive() throws RemoteException {
		return true;
	}

	public ChordMessageInterface getPredecessor() throws RemoteException {
		return predecessor;
	}

	public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
		ChordMessageInterface closestPreceding = this;
		for(ChordMessageInterface finger : fingers) {
			if(isKeyInOpenInterval(key, guid, finger.getId())) {
				return closestPreceding;
			}
			closestPreceding = finger;
		}
		return closestPreceding;
	}

	public void findingNextSuccessor() {
		fingers[0] = this;
		for (int i = 0; i < M; i++) {
			try {
				if (fingers[i].isAlive()) {
					fingers[0] = fingers[i];
				}
			} catch (RemoteException | NullPointerException e) {
				fingers[i] = null;
			}
		}
	}

	public void stabilize() {
		/*
		fixFingers();
		checkPredecessor();
	*/
		try {
			//if (fingers[0] != null) {
				ChordMessageInterface succPred = fingers[0].getPredecessor();

				if (succPred.getId() != this.getId() && isKeyInOpenInterval(succPred.getId(), this.getId(), fingers[0].getId())) {
					fingers[0] = succPred;
					fingers[0].notify(this);
				}
				//if (fingers[0].getId() != getId()) {
				//	fingers[0].notify(this);
				//}
			//}
		} catch (RemoteException | NullPointerException e1) {
			findingNextSuccessor();
		}
	}

	public void notify(ChordMessageInterface chord) throws RemoteException {
		if (predecessor.getId() == this.getId() || isKeyInOpenInterval(chord.getId(), predecessor.getId(), guid))
			// TODO
			// transfer keys not in the range (j,i] to j;
			predecessor = chord;
	}

	public void fixFingers() {
		/*
		try {
			int nextFinger = 0;
			long nextId = this.getId() + 2 << (nextFinger + 1);
			fingers[nextFinger] = locateSuccessor(nextId);

			if (fingers[nextFinger].getId() == guid)
				fingers[nextFinger] = null;
			else
				nextFinger = (nextFinger + 1) % M;
		} catch (RemoteException | NullPointerException e) {
			e.printStackTrace();
		}*/
	}

	public void checkPredecessor() {
		/*
		try {
			if (predecessor != null && !predecessor.isAlive())
				predecessor = null;
		} catch (RemoteException e) {
			predecessor = null;
			// e.printStackTrace();
		}*/
	}
	
	public void leaveRing() throws RemoteException {
		 String path = "./"+guid+"/repository";
         File[] files = new File(path).listFiles();
         for (File file: files) {
             Long guidObject = Long.parseLong(file.getName());
             fingers[0].put(guidObject, get(guidObject));
         } 
	}
	
	public void transferFile(long guidObject, ChordMessageInterface destination) {
		
	}

	public void close() {
		try {
			UnicastRemoteObject.unexportObject(registry, true);
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		String result = "";
		try {
			if (predecessor != null)
				result += "Predecessor: " + predecessor.getId() + "\n";
			if (fingers != null)
				for (int i = 0; i < fingers.length; i++) {
					try {
						result += "Finger " + i + ": " + fingers[i].getId() + "\n";
					} catch (NullPointerException e) {
						fingers[i] = null;
					}
				}
		} catch (RemoteException e) {
			result += "Cannot retrive id\n";
		}
		return result;
	}

	private Boolean isKeyInSemiCloseInterval(long key, long key1, long key2) {
		if (key1 < key2)
			return (key > key1 && key <= key2);
		else
			return (key > key1 || key <= key2);
	}

	private Boolean isKeyInOpenInterval(long key, long key1, long key2) {
		if (key1 < key2)
			return (key > key1 && key < key2);
		else
			return (key > key1 || key < key2);
	}
}
