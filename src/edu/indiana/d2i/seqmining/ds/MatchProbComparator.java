package edu.indiana.d2i.seqmining.ds;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class MatchProbComparator implements Comparator<Map.Entry<Long, Float>> {

	@Override
	public int compare(Entry<Long, Float> o1, Entry<Long, Float> o2) {
		// TODO Auto-generated method stub
		return o2.getValue().compareTo(o1.getValue());
	}

}
