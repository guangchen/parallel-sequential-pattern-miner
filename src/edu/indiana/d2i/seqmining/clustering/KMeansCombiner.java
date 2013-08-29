package edu.indiana.d2i.seqmining.clustering;

import java.util.Iterator;
import java.util.Map;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.types.BytesValue;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;

public class KMeansCombiner implements Combiner {

	private EventSpaceCentroids centroids;

	@Override
	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		// TODO Auto-generated method stub

		/* There should be a single value here */
		assert (keyValues.size() == 1);

		Iterator<Key> iter = keyValues.keySet().iterator();
		Key key = iter.next();
		BytesValue val = (BytesValue) keyValues.get(key);

		try {

			centroids = new EventSpaceCentroids(val.getBytes());

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	@Override
	public void configure(JobConf jobConf) throws TwisterException {
		// TODO Auto-generated method stub

	}

	public EventSpaceCentroids getResults() {
		return centroids;
	}

}
