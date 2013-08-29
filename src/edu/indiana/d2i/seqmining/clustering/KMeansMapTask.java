package edu.indiana.d2i.seqmining.clustering;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringKey;
import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Centroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.ClusterCenter;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Point;
import edu.indiana.d2i.seqmining.util.ClusteringUtils;

public class KMeansMapTask implements MapTask {

	private static String REDUCE_KEY = "kmeans-map-to-reduce-key";
	private FileData fileData;
	private DataSet dataset;
	private Map<Integer, List<Event>> eventGroup;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		// TODO Auto-generated method stub

		fileData = (FileData) mapConf.getDataPartition();

		try {
			dataset = new DataSet(fileData.getFileName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new TwisterException(e);
		}

		eventGroup = ClusteringUtils.dataset2EventGroup(dataset.getExamples());
	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// TODO Auto-generated method stub

		try {
			EventSpaceCentroids eventSpaceCentroids = new EventSpaceCentroids(
					val.getBytes());
			Map<Integer, Centroids> centroids = eventSpaceCentroids
					.getAllCentroids();

			EventSpaceCentroids currentIter = new EventSpaceCentroids();

			for (Map.Entry<Integer, Centroids> entry : centroids.entrySet()) {

				/**
				 * if that event type is already converged, then just continue
				 */
				if (entry.getValue().isConverged()) {
					continue;
				}

				/* initialize centroids */
				Centroids centers = createCentroids(entry.getKey(), entry
						.getValue().getCentroids().size(), 0f, 0f, 0L);

				/**
				 * the dataset of this map task doesn't contain events of this
				 * type, the possibility of this situation is barely low. Note
				 * this check needs be conducted after above initialization code
				 */
				if (null == eventGroup.get(entry.getKey())) {
					// need to set the centers
					currentIter.addCentroidsByType(entry.getKey(), centers);
					continue;
				}

				for (Event event : eventGroup.get(entry.getKey())) {
					int index = findClosest(event, entry.getValue());

					ClusterCenter c = centers.getCentroids().get(index);

					/**
					 * coordinates are accumulated values
					 */
					c.setCenter(
							c.getCenter().getXcoord() + event.getStartTime(),
							c.getCenter().getYcoord() + event.getDuration());
					c.addNumPoints(1);
				}

				currentIter.addCentroidsByType(entry.getKey(), centers);
			}

			// This algorithm uses only one reduce task, so we only need one key

			collector.collect(new StringKey(REDUCE_KEY), new BytesValue(
					currentIter.getBytes()));

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	/**
	 * 
	 * @param event
	 * @param centroids
	 * @return index of the closes centroid
	 */
	private static int findClosest(Event event, Centroids centroids) {
		/* types should be equal */
		assert event.getEventType() == centroids.getEventType();

		int index = -1;

		List<ClusterCenter> centers = centroids.getCentroids();
		float minDist = Float.MAX_VALUE;

		for (int i = 0; i < centers.size(); i++) {
			float dist = getDist(event, centers.get(i));
			if (dist < minDist) {
				minDist = dist;
				index = i;
			}
		}

		return index;
	}

	private static float getDist(Event event, ClusterCenter center) {
		float xDist = event.getStartTime() - center.getCenter().getXcoord();
		float yDist = event.getDuration() - center.getCenter().getYcoord();

		return xDist * xDist + yDist * yDist;
	}

	private static Centroids createCentroids(int eventType, int numCenters,
			float xcoord, float ycoord, long numPoints) {
		Centroids centroids = new Centroids(eventType);

		for (int i = 0; i < numCenters; i++) {
			centroids.addCentroid(new ClusterCenter(new Point(xcoord, ycoord),
					numPoints));
		}

		return centroids;
	}

}
