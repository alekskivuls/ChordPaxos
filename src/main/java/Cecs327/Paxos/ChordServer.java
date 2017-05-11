package Cecs327.Paxos;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class ChordServer {
	int port;
	static Chord chord;

	private long md5(String objectName) {
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(objectName.getBytes());
			BigInteger bigInt = new BigInteger(1, m.digest());
			return Math.abs(bigInt.longValue());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public ChordServer(int p) {
		port = p;

		Timer timer1 = new Timer();
		timer1.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				long guid = md5("" + port);

				try {
					chord = new Chord(port, guid);

					Files.createDirectories(Paths.get(guid + "/repository"));
				} catch (IOException e) {
					e.printStackTrace();

				}
				System.out.println(
						"Usage: \n\tjoin <ip> <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./"
								+ guid + "/file");
				System.out.println("\tread <file>\n\tdelete <file>\n\tprint");

				Scanner scan = new Scanner(System.in);
				String delims = "[ ]+";
				String command = "";
				while (true) {
					String text = scan.nextLine();
					String[] tokens = text.split(delims);
					if (tokens[0].equals("join") && tokens.length == 3) {
						try {
							int portToConnect = Integer.parseInt(tokens[2]);

							chord.joinRing(tokens[1], portToConnect);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					if (tokens[0].equals("print")) {
						System.out.println(chord);
					}
					if (tokens[0].equals("write") && tokens.length == 2) {
						try {
							long guidObject = md5(tokens[1]);
							// If you are using windows you have to use
							// fileName = ".\\"+ port +"\\"+Integer.parseInt(tokens[1]); // path to file
							String fileName = "./" + guid + "/" + tokens[1]; // path to file
							InputStream file = new FileStream(fileName);
							ChordMessageInterface peer = chord.locateSuccessor(guidObject);
							peer.put(guidObject, file); // put file into ring
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					if (tokens[0].equals("read") && tokens.length == 2) {
						long guidObject = md5(tokens[1]);
						// If you are using windows you have to use
						// fileName = ".\\"+ port +"\\"+Integer.parseInt(tokens[1]); // path to file
						try {
							String fileName = "./" + guid + "/" + tokens[1]; // path to file
							ChordMessageInterface peer = chord.locateSuccessor(guidObject);
							InputStream inputStream = peer.get(guidObject); // put file into ring
							int i = 0;

							FileOutputStream output = new FileOutputStream(fileName);
							while (inputStream.available() > 0)
								output.write(inputStream.read());

						} catch (IOException e) {
							System.out.println(e);
						}

					}
					if (tokens[0].equals("delete") && tokens.length == 2) {
						try {
							long guidObject = md5(tokens[1]);
							ChordMessageInterface peer = chord.locateSuccessor(guidObject);
							peer.delete(guidObject);
						} catch (IOException e) {
							System.out.println(e);
						}
					}
				}
			}
		}, 1000, 1000);
	}

	public static void main(String args[]) {
		if (args.length < 1) {
			throw new IllegalArgumentException("Parameter: <port>");
		}
		try {
			ChordServer chordUser = new ChordServer(Integer.parseInt(args[0]));
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("Forced to disconnect, now leaving");
                    try {
						ChordServer.chord.leaveRing();
					} catch (RemoteException e) {
						e.printStackTrace();
					}
            }));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
