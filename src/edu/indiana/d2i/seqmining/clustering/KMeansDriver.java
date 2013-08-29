package edu.indiana.d2i.seqmining.clustering;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.util.ClusteringUtils;

public class KMeansDriver {
	private static String usage = "Usage: java edu.indiana.d2i.seqmining.clustering.KMeansDriver"
			+ " <seed centroid file> <num map tasks> <partition file> <property file>";

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String partitionFile, int numMapTasks,
			String centroidFilePath, Properties prop) throws TwisterException,
			IOException {

		int numReducers = 1; // we need only one reducer

		// job configurations
		JobConf jobConf = new JobConf("kmeans-map-reduce-"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(KMeansMapTask.class);
		jobConf.setReducerClass(KMeansReduceTask.class);
		jobConf.setCombinerClass(KMeansCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		// jobConf.setFaultTolerance();

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);

		// load centroids file
		EventSpaceCentroids centroids = ClusteringUtils
				.loadFromCentroidFile(centroidFilePath);

		boolean converged = false;
		boolean reachedMaxIter = false;
		int loopCount = 0;
		int numMaxIteration = Integer.parseInt(prop
				.getProperty(Constants.CLUSTERING_MAX_NUM_ITER));

		float threshold = Float.parseFloat(prop
				.getProperty(Constants.CLUSTERING_CONVERGE_THRESHOLD));

		TwisterMonitor monitor = null;

		while (!(converged || reachedMaxIter)) {
			monitor = driver.runMapReduceBCast(centroids);
			monitor.monitorTillCompletion();

			EventSpaceCentroids newCentroids = ((KMeansCombiner) driver
					.getCurrentCombiner()).getResults();

			loopCount++;
			System.out.println("Done iteration " + loopCount);

			if (ClusteringUtils.isConverged(centroids, newCentroids, threshold)) {
				System.out.println("Converged at iteration # " + loopCount);
				converged = true;
			}

			if (loopCount >= numMaxIteration) {
				System.out.println("Reached maximum iteration # "
						+ numMaxIteration + ", going to exit the loop");
				reachedMaxIter = true;
			}

			// for next iteration
			centroids = newCentroids;

			// post-adjust centroids
			ClusteringUtils.adjustESCentroids(centroids);
		}

		// save centroids to file
		ClusteringUtils.saveCentroidToFile(centroids,
				prop.getProperty(Constants.CLUSTERING_OUTFILE_PATH));

		// close driver
		driver.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		// TODO Auto-generated method stub

		if (args.length != 4) {
			System.err.println(usage);
			System.exit(1);
		}

		String centroidFilePath = args[0];
		int numMapTasks = Integer.parseInt(args[1]);
		String partitionFile = args[2];

		Properties prop = new Properties();
		prop.load(new FileInputStream(args[3]));

		KMeansDriver driver = null;

		try {
			driver = new KMeansDriver();
			double beginTime = System.currentTimeMillis();
			driver.driveMapReduce(partitionFile, numMapTasks, centroidFilePath,
					prop);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("Kmeans clustering took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
