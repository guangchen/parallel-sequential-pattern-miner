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

public class SeqPatternInAdjust implements Value {
	
	public static class EventInAdjust {
		private int eventType;
		private float sumedStartTime;
		private float sumedEndTime;
		/* number of events over which to get the sum */
		private int numEvents;

		public EventInAdjust(int eventType, float sumedStartTime,
				float sumedEndTime, int numEvents) {
			super();
			this.eventType = eventType;
			this.sumedStartTime = sumedStartTime;
			this.sumedEndTime = sumedEndTime;
			this.numEvents = numEvents;
		}

		public float getSumedStartTime() {
			return sumedStartTime;
		}

		public float getSumedEndTime() {
			return sumedEndTime;
		}

		public int getNumEvents() {
			return numEvents;
		}

		public void setSumedStartTime(float sumedStartTime) {
			this.sumedStartTime = sumedStartTime;
		}

		public void setSumedEndTime(float sumedEndTime) {
			this.sumedEndTime = sumedEndTime;
		}

		public void setNumEvents(int numEvents) {
			this.numEvents = numEvents;
		}

		public int getEventType() {
			return eventType;
		}

	}

	private List<EventInAdjust> patternInAdjust;

	public SeqPatternInAdjust() {
		patternInAdjust = new ArrayList<EventInAdjust>();
	}

	public SeqPatternInAdjust(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public void addEvent(EventInAdjust e) {
		patternInAdjust.add(e);
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read pattern */
			int numEvents = din.readInt();

			patternInAdjust = new ArrayList<EventInAdjust>(numEvents);

			for (int i = 0; i < numEvents; i++) {
				EventInAdjust event = new EventInAdjust(din.readInt(),
						din.readFloat(), din.readFloat(), din.readInt());
				patternInAdjust.add(event);
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
			/* write out pattern */

			// write out # of events
			dout.writeInt(patternInAdjust.size());

			// write out each event
			for (int i = 0; i < patternInAdjust.size(); i++) {
				// write out event type
				dout.writeInt(patternInAdjust.get(i).getEventType());

				// write out summed start time
				dout.writeFloat(patternInAdjust.get(i).getSumedStartTime());

				// write out summed end time
				dout.writeFloat(patternInAdjust.get(i).getSumedEndTime());

				// write out number of summed events
				dout.writeInt(patternInAdjust.get(i).getNumEvents());
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

	public List<EventInAdjust> getPatternInAdjust() {
		return patternInAdjust;
	}
}
