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

/**
 * This class is used as input to generate longer candidate patterns of length K
 * + 1 from patterns of length K and single event patterns. Note: an empty
 * singleEventPatterns indicates the initial condition that variable
 * currentPatterns stores length 1 candidate patterns.
 * 
 * @author Guangchen
 * 
 */
public class CPatternInput implements Value {
	/* patterns that have only single event */
	List<SequentialPattern> singleEventPatterns;

	/* current longest patterns of length K */
	List<SequentialPattern> currentPatterns;

	public CPatternInput(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public CPatternInput() {
		singleEventPatterns = new ArrayList<SequentialPattern>();
		currentPatterns = new ArrayList<SequentialPattern>();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("---single event patters---\n");
		for (SequentialPattern p : singleEventPatterns) {
			sb.append(p.toString());
		}
		sb.append("---single event patters---\n");

		sb.append("---current longest patters---\n");
		for (SequentialPattern p : currentPatterns) {
			sb.append(p.toString());
		}
		sb.append("---current longest patters---\n");
		
		return sb.toString();
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read single event patterns */
			int size = din.readInt();

			singleEventPatterns = new ArrayList<SequentialPattern>(size);

			for (int i = 0; i < size; i++) {
				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				singleEventPatterns.add(new SequentialPattern(bytesArray));
			}

			/* read current longest patterns */
			size = din.readInt();

			currentPatterns = new ArrayList<SequentialPattern>(size);
			for (int i = 0; i < size; i++) {
				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				currentPatterns.add(new SequentialPattern(bytesArray));
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
			/* write out single event patterns */

			// write out # of single event patterns
			dout.writeInt(singleEventPatterns.size());

			// write out each single event pattern
			for (int i = 0; i < singleEventPatterns.size(); i++) {
				byte[] bytes = singleEventPatterns.get(i).getBytes();

				// write size
				dout.writeInt(bytes.length);

				// write value
				dout.write(bytes);
			}

			/* write out current longest patterns */

			// write out # of current longest patterns
			dout.writeInt(currentPatterns.size());

			// write out each current pattern
			for (int i = 0; i < currentPatterns.size(); i++) {
				byte[] bytes = currentPatterns.get(i).getBytes();

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

	public List<SequentialPattern> getSingleEventPatterns() {
		return singleEventPatterns;
	}

	public void setSingleEventPatterns(
			List<SequentialPattern> singleEventPatterns) {
		this.singleEventPatterns = singleEventPatterns;
	}

	public List<SequentialPattern> getCurrentPatterns() {
		return currentPatterns;
	}

	public void setCurrentPatterns(List<SequentialPattern> currentPatterns) {
		this.currentPatterns = currentPatterns;
	}
}
