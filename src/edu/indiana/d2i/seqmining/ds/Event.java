package edu.indiana.d2i.seqmining.ds;

public class Event implements Comparable<Event> {
	private int eventType;
	private float startTime;
	private float duration;
	private float endTime;
	private float centroid;

	public Event(int eventType, float startTime, float duration) {
		super();
		this.eventType = eventType;
		this.startTime = startTime;
		this.duration = duration;
		this.endTime = this.startTime + this.duration;
		this.centroid = this.startTime + this.duration / 2.0f;
	}

	public int getEventType() {
		return eventType;
	}

	public float getStartTime() {
		return startTime;
	}

	public float getDuration() {
		return duration;
	}

	public float getEndTime() {
		return endTime;
	}

	public float getCentroid() {
		return centroid;
	}

	@Override
	public int compareTo(Event e) {
		// natural order by starting time then by ending time then by
		// lexicographical order of event type

		int res = Float.valueOf(startTime).compareTo(e.getStartTime());

		if (res != 0)
			return res;

		res = Float.valueOf(endTime).compareTo(e.getEndTime());

		if (res != 0)
			return res;

		return Integer.valueOf(eventType).compareTo(e.getEventType());
	}

}
