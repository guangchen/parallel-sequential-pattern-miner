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

public class PatternSet implements Value {

	private List<SequentialPattern> patterns;

	public PatternSet(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public PatternSet() {
		patterns = new ArrayList<SequentialPattern>();
	}

	public boolean isEmpty() {
		return (patterns == null) || (patterns.size() == 0);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("---pattern set---\n");
		for (SequentialPattern p : patterns) {
			sb.append(p.toString());
		}
		sb.append("---pattern set---\n");

		return sb.toString();
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read patterns */
			int size = din.readInt();

			patterns = new ArrayList<SequentialPattern>(size);

			for (int i = 0; i < size; i++) {
				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				patterns.add(new SequentialPattern(bytesArray));
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

	public void addPattern(SequentialPattern pattern) {
		patterns.add(pattern);
	}

	public List<SequentialPattern> getPatterns() {
		return patterns;
	}

	public void setPatterns(List<SequentialPattern> patterns) {
		this.patterns = patterns;
	}
}
