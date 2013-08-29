package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class PatternInAdjustSet implements Value {

	private List<SeqPatternInAdjust> patterns;

	public PatternInAdjustSet(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public PatternInAdjustSet() {
		patterns = new ArrayList<SeqPatternInAdjust>();
	}

	public void addPattern(SeqPatternInAdjust pattern) {
		patterns.add(pattern);
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read patterns */
			int size = din.readInt();

			patterns = new ArrayList<SeqPatternInAdjust>(size);

			for (int i = 0; i < size; i++) {
				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				patterns.add(new SeqPatternInAdjust(bytesArray));
			}

			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	public byte[] getBytes() throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		try {
			/* write out patterns */

			// write out # of patterns
			dout.writeInt(patterns.size());

			// write out each pattern
			for (int i = 0; i < patterns.size(); i++) {
				byte[] bytes = patterns.get(i).getBytes();

				// write size
				dout.writeInt(bytes.length);

				// write value
				dout.write(bytes);
			}

			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout.close();
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}

		return marshalledBytes;
	}

	public List<SeqPatternInAdjust> getPatterns() {
		return patterns;
	}

}
