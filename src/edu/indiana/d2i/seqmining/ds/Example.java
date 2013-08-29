package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class Example implements Value {
	private long id;
	/* interval of the whole example */
	private float interval;
	private List<Event> events;
	/* mapping between event type and corresponding list of events */
	private Map<Integer, List<Event>> eventsByType;

	public Example(long id, float interval) {
		super();
		this.id = id;
		this.interval = interval;
		events = new ArrayList<Event>();
		eventsByType = new HashMap<Integer, List<Event>>();
	}

	public Example(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public void addEvent(Event e) {
		events.add(e);

		List<Event> lst = eventsByType.get(e.getEventType());

		if (lst == null) {
			lst = new ArrayList<Event>();
			eventsByType.put(e.getEventType(), lst);
		}

		lst.add(e);
	}

	/**
	 * number of events in the example
	 * 
	 * @return
	 */
	public int getNumEvents() {
		return events.size();
	}

	/**
	 * get all events of the specified "eventType"
	 * 
	 * @param eventType
	 * @param sort
	 *            whether to sort the list
	 * @param comparator
	 * @return
	 */
	public List<Event> getEventsByType(int eventType, boolean sort,
			Comparator<Event> comparator) {
		List<Event> events = eventsByType.get(eventType);
		if (!sort) {
			return events;
		}

		if ((events != null) && (events.size() > 1)) {

			if (comparator != null) {
				// sort by user specified comparator
				Collections.sort(events, comparator);
			} else {
				// sort by natural order
				Collections.sort(events);
			}
		}

		return events;
	}

	public long getId() {
		return id;
	}

	public float getInterval() {
		return interval;
	}

	public List<Event> getEvents() {
		return events;
	}

	public Map<Integer, List<Event>> getEventsByType() {
		return eventsByType;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append(String.format("exampleId=%d duration=%f%n", id, interval));
		StringBuilder eventTypes = new StringBuilder();
		StringBuilder startTimes = new StringBuilder();
		StringBuilder durations = new StringBuilder();

		for (Event ev : events) {
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
			/* read example id */
			id = din.readLong();

			/* read interval */
			interval = din.readFloat();

			/* read events */
			int size = din.readInt();

			events = new ArrayList<Event>(size);
			eventsByType = new HashMap<Integer, List<Event>>();

			for (int i = 0; i < size; i++) {
				this.addEvent(new Event(din.readInt(), din.readFloat(), din
						.readFloat()));

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
			/* write out example id */
			dout.writeLong(id);

			/* write out interval */
			dout.writeFloat(interval);

			// write out # of events
			dout.writeInt(events.size());

			// write out each event
			for (int i = 0; i < events.size(); i++) {
				// write out event type
				dout.writeInt(events.get(i).getEventType());

				// write out start time
				dout.writeFloat(events.get(i).getStartTime());

				// write out duration
				dout.writeFloat(events.get(i).getDuration());
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
