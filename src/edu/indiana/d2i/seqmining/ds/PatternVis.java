package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class PatternVis implements Value {

	public static class MatchedExample implements Value {
		Example example;
		float matchingProb;

		public MatchedExample(Example example, float matchingProb) {
			super();
			this.example = example;
			this.matchingProb = matchingProb;
		}

		public MatchedExample(byte[] bytes) throws SerializationException {
			this.fromBytes(bytes);
		}

		public Example getExample() {
			return example;
		}

		public float getMatchingProb() {
			return matchingProb;
		}

		@Override
		public void fromBytes(byte[] bytes) throws SerializationException {
			// TODO Auto-generated method stub

			ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
			DataInputStream din = new DataInputStream(baInputStream);

			try {
				/* read patterns */
				matchingProb = din.readFloat();

				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);
				example = new Example(bytesArray);

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

				// write out matching prob
				dout.writeFloat(matchingProb);

				// write out example
				byte[] bytes = example.getBytes();

				// write out size
				dout.writeInt(bytes.length);
				// write out value
				dout.write(bytes);

				dout.flush();
				marshalledBytes = baOutputStream.toByteArray();
				baOutputStream = null;
				dout.close();
			} catch (IOException ioe) {
				throw new SerializationException(ioe);
			}

			return marshalledBytes;
		}

	}

	private SequentialPattern pattern;
	private List<MatchedExample> matchedExamples;
	/* all event types */
	private Set<Integer> allEventTypes;

	public PatternVis() {
		super();
		pattern = null;
		matchedExamples = new ArrayList<MatchedExample>();
		allEventTypes = new HashSet<Integer>();
	}

	public PatternVis(SequentialPattern pattern) {
		super();
		this.pattern = pattern;
		matchedExamples = new ArrayList<MatchedExample>();
		allEventTypes = new HashSet<Integer>();
	}

	public PatternVis(byte[] bytes) throws SerializationException {
		super();
		this.fromBytes(bytes);
	}

	public void addEventType(int eventType) {
		allEventTypes.add(eventType);
	}

	public SequentialPattern getPattern() {
		return pattern;
	}

	public void setPattern(SequentialPattern pattern) {
		this.pattern = pattern;
	}

	public void addMatchedExample(MatchedExample ex) {
		matchedExamples.add(ex);
	}

	public List<MatchedExample> getMatchedExamples() {
		return matchedExamples;
	}

	public void setMatchedExamples(List<MatchedExample> matchedExamples) {
		this.matchedExamples = matchedExamples;
	}

	public Set<Integer> getAllEventTypes() {
		return allEventTypes;
	}

	public void setAllEventTypes(Set<Integer> allEventTypes) {
		this.allEventTypes = allEventTypes;
	}
	
	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read pattern */
			int bytesLength = din.readInt();
			byte[] bytesArray = new byte[bytesLength];
			din.read(bytesArray);
			pattern = new SequentialPattern(bytesArray);

			/* read matched examples */
			int numMatchedEx = din.readInt();
			matchedExamples = new ArrayList<MatchedExample>(numMatchedEx);

			for (int i = 0; i < numMatchedEx; i++) {
				bytesLength = din.readInt();
				bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				matchedExamples.add(new MatchedExample(bytesArray));
			}

			/* read event types */
			int numTypes = din.readInt();

			allEventTypes = new HashSet<Integer>(numTypes);
			for (int i = 0; i < numTypes; i++) {
				allEventTypes.add(din.readInt());
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

			// write out pattern
			byte[] bytes = pattern.getBytes();

			// write size
			dout.writeInt(bytes.length);
			// write value
			dout.write(bytes);

			// write out each matched example
			dout.writeInt(matchedExamples.size());

			for (int i = 0; i < matchedExamples.size(); i++) {
				bytes = matchedExamples.get(i).getBytes();

				// write size
				dout.writeInt(bytes.length);

				// write value
				dout.write(bytes);
			}

			// write out all event types
			dout.writeInt(allEventTypes.size());

			for (Integer type : allEventTypes) {
				dout.writeInt(type);
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
}
