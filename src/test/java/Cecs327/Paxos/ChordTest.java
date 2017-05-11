package Cecs327.Paxos;

import static org.junit.Assert.*;

import java.rmi.RemoteException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChordTest {

	static Chord[] chords;
	static final int defaultPort = 8080;

	@Before
	public void initChords() throws Exception {
		int maxChords = (int) Math.pow(2, Chord.M);
		chords = new Chord[maxChords];
		for (int i = 0; i < chords.length; i++) {
			int port = defaultPort + i;
			chords[i] = new Chord(port, i);
			System.out.println(i + " is starting RMI at port=" + port);
		}
	}

	@After
	public void closeChords() {
		for (Chord chord : chords)
			chord.close();
		chords = null;
	}

	@Test
	public void joinOneChord() throws RemoteException, InterruptedException {
		chords[0].joinRing("localhost", defaultPort + 1);
		assertEquals(1, chords[0].getSuccessor().getId());
		assertEquals(0, chords[1].getPredecessor().getId());

		Thread.sleep(Chord.STABILIZE_TIMER);

		assertEquals(1, chords[0].getPredecessor().getId());
		assertEquals(0, chords[1].getSuccessor().getId());

		printChords();
	}

	@Test
	public void joinAllChords() throws RemoteException, InterruptedException {
		System.out.println("Join all chords");
		for (int i = 1; i < chords.length; i++) {
			chords[i].joinRing("localhost", defaultPort);
			Thread.sleep(Chord.STABILIZE_TIMER);
		}
		printChords();

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
//		chords[2].leaveRing();
//		Thread.sleep(Chord.STABILIZE_TIMER);
//		printChords();
	}
	
	@Test
	public void concurrentJoinAllChords() throws RemoteException, InterruptedException {
		System.out.println("Concurrently joining all chords");
		for (int i = 1; i < chords.length; i++) {
			chords[i].joinRing("localhost", defaultPort);
		}
		Thread.sleep(Chord.STABILIZE_TIMER * chords.length);
		Thread.sleep(1000);
		printChords();

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
	}

	private void printChords() {
		int i = 0;
		for (Chord chord : chords) {
			System.out.println("Chord " + i++);
			System.out.println(chord);
		}
	}
}
