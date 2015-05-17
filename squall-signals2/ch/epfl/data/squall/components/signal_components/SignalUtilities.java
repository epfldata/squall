package ch.epfl.data.squall.components.signal_components;

public class SignalUtilities {
	
	public static int DISTRIBUTION_SIGNAL=0;
	public static int HARMONIZER_SIGNAL=1;
	
	
	public static byte[] createSignal(int signalNum, byte[] payload){
		
		byte[] signal= toBytes(signalNum);
		byte[] newArray = new byte[signal.length + payload.length];
		System.arraycopy(signal, 0, newArray, 0, signal.length);
		System.arraycopy(payload, 0, newArray, 4, payload.length);
		return newArray;
	}
	
	
	public static int byteArrayToInt(byte[] b) {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (b[i] & 0x000000FF) << shift;
		}
		return value;
	}
	
	public static byte[] toBytes(int i) {
		byte[] result = new byte[4];
		result[0] = (byte) (i >> 24);
		result[1] = (byte) (i >> 16);
		result[2] = (byte) (i >> 8);
		result[3] = (byte) (i /* >> 0 */);
		return result;
	    }

}
