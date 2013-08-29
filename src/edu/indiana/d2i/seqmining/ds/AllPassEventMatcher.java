package edu.indiana.d2i.seqmining.ds;

public class AllPassEventMatcher {

	public boolean match(Event ev1, Event ev2) {
		// TODO Auto-generated method stub
		return ev1.getEventType() == ev2.getEventType();
	}

}
