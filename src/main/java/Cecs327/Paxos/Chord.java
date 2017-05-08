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

	Registry registry; // rmi registry for lookup the remote objects.
	ChordMessageInterface successor;
	ChordMessageInterface predecessor;
	ChordMessageInterface[] fingers;
	int nextFinger;
	long guid; // GUID (i)

	public Chord(int port, long guid) throws RemoteException {
		this.guid = guid;
		fingers = new ChordMessageInterface[M];
		for (int i = 0; i < M; i++) {
			fingers[i] = null;
		}

		predecessor = null;
		successor = this;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				stabilize();
				fixFingers();
				checkPredecessor();
			}
		}, 500, 500);
		try {
			// create the registry and bind the name and object.
			System.out.println(guid + " is starting RMI at port=" + port);
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
			predecessor = null;
			successor = chord.locateSuccessor(this.getId());
			System.out.println("Joining ring");
		} catch (RemoteException | NotBoundException e) {
			successor = this;
		}
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
		System.out.println(fileName);
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

	public ChordMessageInterface locateSuccessor(long key) throws RemoteException {
		if (key == guid)
			throw new IllegalArgumentException("Key must be distinct that  " + guid);
		if (successor.getId() != guid) {
			if (isKeyInSemiCloseInterval(key, guid, successor.getId()))
				return successor;
			ChordMessageInterface j = closestPrecedingNode(key);

			if (j == null)
				return null;
			return j.locateSuccessor(key);
		}
		return successor;
	}

	public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
		return predecessor;
	}

	public void findingNextSuccessor() {
		successor = this;
		for (int i = 0; i < M; i++) {
			try {
				if (fingers[i].isAlive()) {
					successor = fingers[i];
				}
			} catch (RemoteException | NullPointerException e) {
				fingers[i] = null;
			}
		}
	}

	public void stabilize() {
		try {
			if (successor != null) {
				ChordMessageInterface x = successor.getPredecessor();

				if (x != null && x.getId() != this.getId()
						&& isKeyInOpenInterval(x.getId(), this.getId(), successor.getId())) {
					successor = x;
				}
				if (successor.getId() != getId()) {
					successor.notify(this);
				}
			}
		} catch (RemoteException | NullPointerException e1) {
			findingNextSuccessor();

		}
	}

	public void notify(ChordMessageInterface j) throws RemoteException {
		if (predecessor == null || (predecessor != null && isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
			// TODO
			// transfer keys not in the range (j,i] to j;
			predecessor = j;
	}

	public void fixFingers() {
		try {
			long nextId = this.getId() + 2 << (nextFinger + 1);
			fingers[nextFinger] = locateSuccessor(nextId);

			if (fingers[nextFinger].getId() == guid)
				fingers[nextFinger] = null;
			else
				nextFinger = (nextFinger + 1) % M;
		} catch (RemoteException | NullPointerException e) {
			e.printStackTrace();
		}
	}

	public void checkPredecessor() {
		try {
			if (predecessor != null && !predecessor.isAlive())
				predecessor = null;
		} catch (RemoteException e) {
			predecessor = null;
			// e.printStackTrace();
		}
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
			if (successor != null)
				result += "successor " + successor.getId() + "\n";
			if (predecessor != null)
				result += "predecessor " + predecessor.getId() + "\n";
			if (fingers != null)
				for (int i = 0; i < fingers.length; i++) {
					try {
						result += "Finger " + i + " " + fingers[i].getId() + "\n";
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
