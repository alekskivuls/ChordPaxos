package Cecs327.Paxos;

import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import Cecs327.Paxos.ChordHashing.HashingFunction;

public class ChordTest {

	static Chord[] chords;
	static final int defaultPort = 8000;

	@Rule
	public ChordWatcher chordWatcher = new ChordWatcher();

	@BeforeClass
	public static void configureChord() {
		Chord.hashFun = HashingFunction.STRICT_BYTE;
		Chord.M = HashingFunction.STRICT_BYTE.getNumBits();
	}

	public static void initChords() {
		int maxChords = (int) Math.pow(2, Chord.M);
		chords = new Chord[maxChords];
		for (int i = 0; i < chords.length; i++) {
			int port = defaultPort + i;
			try {
				chords[i] = new Chord(port, i);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			// System.out.println(i + " is starting RMI at port=" + port);
		}
	}

	public static void closeChords() {
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
	}

	@Test
	public void joinAllChords() throws RemoteException, InterruptedException {
		for (int i = 1; i < chords.length; i++) {
			chords[i].joinRing("localhost", defaultPort);
			Thread.sleep(Chord.STABILIZE_TIMER);
		}

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
	}

	@Test
	public void joinAllChordsRandomly() throws RemoteException, InterruptedException {
		List<Integer> shuffledList = IntStream.range(0, chords.length).boxed().collect(Collectors.toList());
		Collections.shuffle(shuffledList);
		Random rand = new Random();

		for (int i = 1; i < chords.length; i++) {
			chords[shuffledList.get(i)].joinRing("localhost", defaultPort + shuffledList.get(rand.nextInt(i)));
			Thread.sleep(Chord.STABILIZE_TIMER);
		}

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
	}

	@Test
	public void joinAllChordsConcurrently() throws RemoteException, InterruptedException {
		for (int i = 1; i < chords.length; i++) {
			chords[i].joinRing("localhost", defaultPort);
		}
		Thread.sleep(Chord.STABILIZE_TIMER * chords.length);
		Thread.sleep(1000);

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
	}

	@Test
	public void joinAllChordsRandomlyConcurrently() throws RemoteException, InterruptedException {
		List<Integer> shuffledList = IntStream.range(0, chords.length).boxed().collect(Collectors.toList());
		Collections.shuffle(shuffledList);
		Random rand = new Random();

		for (int i = 1; i < chords.length; i++) {
			chords[shuffledList.get(i)].joinRing("localhost", defaultPort + shuffledList.get(rand.nextInt(i)));
		}
		Thread.sleep(Chord.STABILIZE_TIMER * chords.length);

		for (int i = 0; i < chords.length; i++) {
			assertEquals(i - 1 < 0 ? chords.length - 1 : i - 1, chords[i].getPredecessor().getId());
			assertEquals((i + 1) % chords.length, chords[i].getSuccessor().getId());
		}
	}

	public static void printChords() {
		int i = 0;
		for (Chord chord : chords) {
			System.out.println("Chord " + i++);
			System.out.println(chord);
		}
	}

	private class ChordWatcher extends TestWatcher {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println("\nTest Failed\n");
			ChordTest.printChords();
			System.out.println("\nStack Trace\n");
			e.printStackTrace();
		}

		@Override
		protected void starting(Description description) {
			ChordTest.initChords();
			System.out.println(description);
		}

		@Override
		protected void finished(Description description) {
			ChordTest.closeChords();
		}
	}
}
