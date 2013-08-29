package edu.indiana.d2i.seqmining.clustering;

import java.util.List;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Centroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.ClusterCenter;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Point;

public class KMeansReduceTask implements ReduceTask {

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		// TODO Auto-generated method stub

		if (values.size() <= 0) {
			throw new TwisterException("Reduce input error no values.");
		}

		try {
			EventSpaceCentroids aggregatedCentroids = new EventSpaceCentroids();

			for (Value v : values) {
				EventSpaceCentroids partialCentroids = new EventSpaceCentroids(
						v.getBytes());

				for (Map.Entry<Integer, Centroids> entry : partialCentroids
						.getAllCentroids().entrySet()) {

					Centroids centers = aggregatedCentroids
							.getCentroidsByType(entry.getKey());

					if (centers == null) {
						centers = new Centroids(entry.getKey());
						aggregatedCentroids.addCentroidsByType(entry.getKey(),
								centers);
					}

					mergeCentroids(centers, entry.getValue());
				}
			}

			// divide the sum by # of belonging points
			for (Centroids centroids : aggregatedCentroids.getAllCentroids()
					.values()) {
				for (ClusterCenter center : centroids.getCentroids()) {

					if (center.getNumPoints() == 0) {
						// no points belonging to this center, just continue
						continue;
					}

					float newXCoord = center.getCenter().getXcoord()
							/ (float) center.getNumPoints();
					float newYCoord = center.getCenter().getYcoord()
							/ (float) center.getNumPoints();

					center.setCenter(newXCoord, newYCoord);
				}
			}

			// emit final result
			collector.collect(key,
					new BytesValue(aggregatedCentroids.getBytes()));

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	/**
	 * merge c2 into c1
	 * 
	 * @param c1
	 * @param c2
	 */
	private static void mergeCentroids(Centroids c1, Centroids c2) {
		if (c1.getCentroids().size() == 0) {

			List<ClusterCenter> centers = c2.getCentroids();

			for (int i = 0; i < centers.size(); i++) {

				c1.addCentroid(new ClusterCenter(new Point(centers.get(i)
						.getCenter().getXcoord(), centers.get(i).getCenter()
						.getYcoord()), centers.get(i).getNumPoints()));
			}

			return;
		}

		assert c1.getCentroids().size() == c2.getCentroids().size();

		List<ClusterCenter> centersC1 = c1.getCentroids();
		List<ClusterCenter> centersC2 = c2.getCentroids();

		for (int i = 0; i < centersC1.size(); i++) {

			ClusterCenter center1 = centersC1.get(i);
			ClusterCenter center2 = centersC2.get(i);

			float newXCoord = center1.getCenter().getXcoord()
					+ center2.getCenter().getXcoord();
			float newYCoord = center1.getCenter().getYcoord()
					+ center2.getCenter().getYcoord();
			long newNumPoints = center1.getNumPoints() + center2.getNumPoints();

			center1.setCenter(newXCoord, newYCoord);
			center1.setNumPoints(newNumPoints);
		}
	}

}
