package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class SequentialPattern implements Value {
	private String id = "DEFAULT-ID";
	private List<Event> pattern;
	/* mapping between matched example id and its score */
	private Map<Long, Float> matchExScore;
	private float totalScore = 0f;

	public SequentialPattern() {
		super();
		pattern = new ArrayList<Event>();
		matchExScore = new HashMap<Long, Float>();
	}

	public SequentialPattern(String id) {
		super();
		this.id = id;
		pattern = new ArrayList<Event>();
		matchExScore = new HashMap<Long, Float>();
	}

	public SequentialPattern(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	/**
	 * number of events in the example
	 * 
	 * @return
	 */
	public int getNumEvents() {
		return pattern.size();
	}

	public void addEvent(Event e) {
		pattern.add(e);
	}

	public void addMatchedEx(long exampleId, float score) {
		matchExScore.put(exampleId, score);
		totalScore += score;
	}

	public List<Event> getPattern(boolean sort) {
		if (!sort)
			return pattern;

		Collections.sort(pattern);
		return pattern;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<Long, Float> getMatchExScore() {
		return matchExScore;
	}

	public Event getLastEvent() {
		Collections.sort(pattern);
		return pattern.get(pattern.size() - 1);
	}

	public Set<Integer> getEventTypeSet() {
		Set<Integer> eventTypeSet = new HashSet<Integer>();

		for (Event e : pattern) {
			eventTypeSet.add(e.getEventType());
		}

		return eventTypeSet;
	}

	public float getTotalScore() {
		return totalScore;
	}

	public void setTotalScore(float totalScore) {
		this.totalScore = totalScore;
	}

	public Set<Long> getMatchedExampleId() {
		return matchExScore.keySet();
	}

	public void setMatchExScore(Map<Long, Float> matchExScore) {
		this.matchExScore = matchExScore;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append(String.format("patternId=%s totalScore=%f%n", id, totalScore));

		StringBuilder eventTypes = new StringBuilder();
		StringBuilder startTimes = new StringBuilder();
		StringBuilder durations = new StringBuilder();

		for (Event ev : pattern) {
			eventTypes.append(ev.getEventType() + " ");
			startTimes.append(ev.getStartTime() + " ");
			durations.append(ev.getDuration() + " ");
		}

		sb.append(eventTypes.toString() + "\n");
		sb.append(startTimes.toString() + "\n");
		sb.append(durations.toString() + "\n");

		return sb.toString();
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read total score */
			totalScore = din.readFloat();

			/* read pattern id */
			int size = din.readInt();
			byte[] byteArray = new byte[size];
			din.read(byteArray);
			id = new String(byteArray);

			/* read pattern */
			size = din.readInt();

			pattern = new ArrayList<Event>(size);

			for (int i = 0; i < size; i++) {
				Event event = new Event(din.readInt(), din.readFloat(),
						din.readFloat());
				pattern.add(event);
			}

			/* read score table */
			size = din.readInt();

			matchExScore = new HashMap<Long, Float>(size);

			for (int i = 0; i < size; i++) {
				matchExScore.put(din.readLong(), din.readFloat());
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
			/* write out total score */
			dout.writeFloat(totalScore);

			/* write out pattern id */
			byte[] patternId = id.getBytes();
			dout.writeInt(patternId.length);
			dout.write(patternId);

			/* write out pattern */

			// write out # of events
			dout.writeInt(pattern.size());

			// write out each event
			for (int i = 0; i < pattern.size(); i++) {
				// write out event type
				dout.writeInt(pattern.get(i).getEventType());

				// write out start time
				dout.writeFloat(pattern.get(i).getStartTime());

				// write out duration
				dout.writeFloat(pattern.get(i).getDuration());
			}

			/* write out score table */

			// write out # of mappings
			dout.writeInt(matchExScore.size());

			// write out each entry
			for (Map.Entry<Long, Float> entry : matchExScore.entrySet()) {
				// write out example id
				dout.writeLong(entry.getKey());

				// write out score
				dout.writeFloat(entry.getValue());
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
