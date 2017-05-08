package Cecs327.Paxos;

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
			chords[i] = new Chord(defaultPort + i, i);
		}
	}

	@After
	public void closeChords() {
		for (Chord chord : chords)
			chord.close();
		chords = null;
	}

	@Test
	public void printChords() {
		for(Chord chord : chords) {
			System.out.println(chord);
		}
	}

	@Test
	public void test2() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Test2");
	}

}
