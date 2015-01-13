package newt.actor;

import newt.common.ByteArray;
import newt.common.Digest;

/**
 * Represents a record as raw bytes.
 *
 */
public class ByteArrayProvenance implements ProvenanceDataType<ByteArray> {

	byte[] record;
	ByteArray provenance = null;
	String hexStr = null;

	public ByteArrayProvenance(byte[] array, int start, int length) {
		record = new byte[length];
		System.arraycopy(array, start, record, 0, length);
	}
	
	public ByteArrayProvenance(byte[] tuple) {
		this.record = tuple;
	}

	@Override
	public byte[] getBytes() {
		return toProvenance().getBytes();
	}

	@Override
	public ByteArray toProvenance() {

		if( provenance != null ) {
			return provenance;
		}

		byte[] digest = Digest.digest( record );
		provenance = new ByteArray( digest );
		return provenance;
	}

	public static String getHexString(byte[] b, int start, int end) {
		String hexStr = "";
		for (int i=start; i < end; i++) {
			hexStr +=
					Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
		}
		return hexStr;
	}
	
	/**
	 * Returns the hex representation of a byte array.
	 */
	public String getHexString(byte[] b) {
		if (hexStr!=null) {
			return hexStr;
		} else {
			hexStr = "";
			for (int i=0; i < b.length; i++) {
				hexStr +=
						Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
			}
			return hexStr;
		}
	} 
	
	/**
	 * Returns a string representation of the provenance hash value.
	 */
	public String toString() {
		return getHexString(getBytes());
	}
}
