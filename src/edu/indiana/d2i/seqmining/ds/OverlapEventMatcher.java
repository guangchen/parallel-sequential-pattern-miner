package edu.indiana.d2i.seqmining.ds;

/**
 * Two events ev1 and ev2 are considered to be a match when following conditions
 * are met. (1) ev1.type == ev2.type (2) startTime(ev2) < centroid(ev1) (3)
 * endTime(ev2) > centroid(ev1) (4) (length(ev2) - length(ev1)) / length(ev1) <
 * lengthRatio
 * 
 * @author Guangchen
 * 
 */
public class OverlapEventMatcher {

	private float lengthRatio;

	public OverlapEventMatcher(float lengthRatio) {
		super();
		this.lengthRatio = lengthRatio;
	}

	public boolean match(Event ev1, Event ev2) {
		// TODO Auto-generated method stub
		return (ev1.getEventType() == ev2.getEventType())
				&& (ev2.getStartTime() < ev1.getCentroid())
				&& (ev2.getEndTime() > ev1.getCentroid())
				&& (((ev2.getDuration() - ev1.getDuration()) / ev1
						.getDuration()) < lengthRatio);
	}

}
