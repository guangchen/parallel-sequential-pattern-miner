package edu.indiana.d2i.seqmining.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Centroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.ClusterCenter;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Point;
import edu.indiana.d2i.seqmining.ds.Example;

public class ClusteringUtils {

	// variation ratio in terms of average
	private static float VARIATION_RATIO = 0.1f;

	/**
	 * load centroids in event space from text file, which has following format,
	 * <> indicates a line: <# of event types> <event type> <start_time1,
	 * start_time2, ...> <duration1, duration2, ...> <other centroids ...>. The
	 * very first line is the number of event types, then each event type spans
	 * three lines, the first line is the event type, the second line is start
	 * times and the third line is corresponding durations.
	 * 
	 * @param centroidFilePath
	 * @return
	 * @throws IOException
	 */
	public static EventSpaceCentroids loadFromCentroidFile(
			String centroidFilePath) throws IOException {

		EventSpaceCentroids eventSpaceCentoids = new EventSpaceCentroids();

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(centroidFilePath)));

			long numEventTypes = Long.parseLong(reader.readLine().trim());

			for (long i = 0; i < numEventTypes; i++) {

				int eventType = Integer.parseInt(reader.readLine().trim());

				String[] startTimes = reader.readLine().trim().split("\\s+");
				String[] durations = reader.readLine().trim().split("\\s+");

				/* lengths should be equal */
				assert startTimes.length == durations.length;

				Centroids centroids = new Centroids(eventType);

				for (int j = 0; j < startTimes.length; j++) {
					centroids.addCentroid(new ClusterCenter(new Point(Float
							.parseFloat(startTimes[j]), Float
							.parseFloat(durations[j]))));
				}

				eventSpaceCentoids.addCentroidsByType(eventType, centroids);
			}
		} finally {
			if (reader != null)
				reader.close();
		}

		return eventSpaceCentoids;
	}

	/**
	 * save centroids to a file, the file format is the same as file loaded by
	 * loadFromCentroidFile()
	 * 
	 * @param eventSpaceCentoids
	 * @param outputFilePath
	 * @throws IOException
	 */
	public static void saveCentroidToFile(
			EventSpaceCentroids eventSpaceCentoids, String outputFilePath)
			throws IOException {

		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputFilePath)));

			// write # of event types
			writer.write(eventSpaceCentoids.getAllCentroids().size() + "\n");

			for (Map.Entry<Integer, Centroids> entry : eventSpaceCentoids
					.getAllCentroids().entrySet()) {
				// write event type
				writer.write(entry.getKey() + "\n");

				StringBuilder startTime = new StringBuilder();
				StringBuilder duration = new StringBuilder();

				for (ClusterCenter center : entry.getValue().getCentroids()) {
					startTime.append(center.getCenter().getXcoord() + " ");
					duration.append(center.getCenter().getYcoord() + " ");
				}

				startTime.append("\n");
				duration.append("\n");

				// write start time
				writer.write(startTime.toString());
				// write duration
				writer.write(duration.toString());
			}

			writer.flush();

		} finally {
			if (writer != null)
				writer.close();
		}
	}

	public static boolean closeEnough(Centroids first, Centroids second,
			float threshold) {

		/* same type */
		assert first.getEventType() == second.getEventType();

		List<ClusterCenter> fistCentroids = first.getCentroids();
		List<ClusterCenter> secondCentroid = second.getCentroids();

		/* same # of centroids */
		assert fistCentroids.size() == secondCentroid.size();

		for (int i = 0; i < fistCentroids.size(); i++) {
			float xDist = fistCentroids.get(i).getCenter().getXcoord()
					- secondCentroid.get(i).getCenter().getXcoord();
			float yDist = fistCentroids.get(i).getCenter().getYcoord()
					- secondCentroid.get(i).getCenter().getYcoord();

			/*
			 * we don't use square root since its calculation is time consuming,
			 * using the square doesn't affect the correctness of the algorithm
			 */
			float d = xDist * xDist + yDist * yDist;

			if (d > threshold)
				return false;
		}

		return true;
	}

	public static boolean isConverged(EventSpaceCentroids prevIter,
			EventSpaceCentroids curIter, float threshold) {

		boolean converged = true;

		Map<Integer, Centroids> prevIterCentroids = prevIter.getAllCentroids();
		Map<Integer, Centroids> curIterCentroids = curIter.getAllCentroids();

		for (Integer eventType : prevIterCentroids.keySet()) {
			Centroids prev = prevIterCentroids.get(eventType);
			Centroids cur = curIterCentroids.get(eventType);

			/**
			 * since in current iteration, we only deal with clustering of event
			 * types that are not converged yet, we need set back those
			 * converged clusters to current iteration from previous iteration
			 */
			if (prev.isConverged()) {
				curIterCentroids.put(eventType, prev);
			} else {
				// check whether it is converged in current iteration

				if (closeEnough(prev, cur, threshold)) {
					cur.setConverged(true);
				} else {
					converged = false;
				}
			}
		}

		return converged;
	}

	public static Map<Integer, List<Event>> dataset2EventGroup(
			List<Example> examples) {
		Map<Integer, List<Event>> eventGroup = new HashMap<Integer, List<Event>>();

		for (Example ex : examples) {
			for (Event event : ex.getEvents()) {
				List<Event> group = eventGroup.get(event.getEventType());

				if (group == null) {
					group = new ArrayList<Event>();
					eventGroup.put(event.getEventType(), group);
				}

				group.add(event);
			}
		}

		return eventGroup;
	}

	/**
	 * adjust centroids when there are empty clusters
	 * 
	 * @param centroids
	 */
	public static void adjustESCentroids(EventSpaceCentroids centroids) {

		for (Centroids centers : centroids.getAllCentroids().values()) {

			boolean emptyCluster = false;

			for (ClusterCenter c : centers.getCentroids()) {
				if (c.getNumPoints() == 0) {
					emptyCluster = true;
					break;
				}
			}

			// if no empty clusters, just continue
			if (!emptyCluster) {
				continue;
			}

			// set convergence to be false;
			centers.setConverged(false);

			// get total # of data points of this event type in event space
			long numTotalPoints = 0L;
			for (ClusterCenter c : centers.getCentroids()) {
				numTotalPoints += c.getNumPoints();
			}

			// calculate averaged center over all data points
			float averagedX = 0f;
			float averagedY = 0f;

			for (ClusterCenter c : centers.getCentroids()) {
				averagedX += c.getCenter().getXcoord() * c.getNumPoints();
				averagedY += c.getCenter().getYcoord() * c.getNumPoints();
			}

			averagedX /= (float) numTotalPoints;
			averagedY /= (float) numTotalPoints;

			// initialize random number generator
			Random rnd = new Random(System.currentTimeMillis());
			float variationX = averagedX * VARIATION_RATIO;
			float variationY = averagedY * VARIATION_RATIO;

			// adjust centers
			for (ClusterCenter c : centers.getCentroids()) {
				// don't change non-empty cluster
				if (c.getNumPoints() != 0) {
					continue;
				}

				// add variation
				float xcoord = averagedX + (float) rnd.nextGaussian()
						* variationX;
				float ycoord = averagedY + (float) rnd.nextGaussian()
						* variationY;

				if (xcoord <= 0) {
					xcoord = averagedX;
				}

				if (ycoord <= 0) {
					ycoord = averagedY;
				}

				c.setCenter(xcoord, ycoord);
			}
		}
	}
}
