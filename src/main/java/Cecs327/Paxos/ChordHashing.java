package Cecs327.Paxos;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChordHashing {
	enum HashingFunction {
		MD5(128) {
			@Override
			long hash(String message) {
				try {
					MessageDigest m = MessageDigest.getInstance("MD5");
					m.reset();
					m.update(message.getBytes());
					BigInteger bigInt = new BigInteger(1, m.digest());
					return Math.abs(bigInt.longValue());
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				return 0;
			}

		},
		SHA1(160) {
			@Override
			long hash(String message) {
				try {
					MessageDigest m = MessageDigest.getInstance("SHA-1");
					m.reset();
					m.update(message.getBytes());
					BigInteger bigInt = new BigInteger(1, m.digest());
					return Math.abs(bigInt.longValue());
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				return 0;
			}

		},
		SHA256(256) {
			@Override
			long hash(String message) {
				try {
					MessageDigest m = MessageDigest.getInstance("SHA-256");
					m.reset();
					m.update(message.getBytes());
					BigInteger bigInt = new BigInteger(1, m.digest());
					return Math.abs(bigInt.longValue());
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				return 0;
			}
		},
		STRICT_INTEGER(Integer.SIZE) {
			@Override
			long hash(String message) {
				try {
					return Integer.valueOf(message);
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
				return 0;
			}
		},
		STRICT_BYTE(Byte.SIZE) {
			@Override
			long hash(String message) {
				try {
					return Byte.valueOf(message);
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
				return 0;
			}
		};;

		private int numBits;

		HashingFunction(int numBits) {
			this.numBits = numBits;
		}

		int getNumBits() {
			return numBits;
		};

		abstract long hash(String message);
	}
}