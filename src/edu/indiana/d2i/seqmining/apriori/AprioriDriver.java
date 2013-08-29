package edu.indiana.d2i.seqmining.apriori;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;

import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.CPatternInput;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.util.AprioriUtils;

public class AprioriDriver {

	private static String usage = "Usage: java edu.indiana.d2i.seqmining.apriori.AprioriDriver"
			+ " <converged centroid file> <num map tasks> <partition file> <property file>";

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String partitionFile, int numMapTasks,
			String centroidFilePath, Properties prop) throws TwisterException,
			IOException, SerializationException {

		int numReducers = 1; // we need only one reducer

		// job configurations
		JobConf jobConf = new JobConf("apriori-like-"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(AprioriMapTask.class);
		jobConf.setReducerClass(AprioriReduceTask.class);
		jobConf.setCombinerClass(AprioriCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		// jobConf.setFaultTolerance();

		// set algorithm related properties
		jobConf.addProperty(Constants.APRIORI_EVENT_MATCH_USE_PENALTY,
				prop.getProperty(Constants.APRIORI_EVENT_MATCH_USE_PENALTY));
		jobConf.addProperty(
				Constants.APRIORI_EVENT_MATCH_PENALTY_THRESHOLD,
				prop.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_THRESHOLD));
		jobConf.addProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_FACTOR,
				prop.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_FACTOR));
		jobConf.addProperty(Constants.APRIORI_PATTERN_MATCH_THRESHOLD,
				prop.getProperty(Constants.APRIORI_PATTERN_MATCH_THRESHOLD));
		jobConf.addProperty(Constants.DATASET_NUM_EXAMPLES,
				prop.getProperty(Constants.DATASET_NUM_EXAMPLES));
		jobConf.addProperty(Constants.APRIORI_MIN_SUP_PROB,
				prop.getProperty(Constants.APRIORI_MIN_SUP_PROB));

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);

		// load centroids file to generate candidate single event patterns
		CPatternInput candidatePanInput = AprioriUtils
				.loadPatternSetFromFile(centroidFilePath);

		boolean terminated = false;
		boolean reachedMaxPatLen = false;

		/* threshold used to remove 'prefix' and 'included' patterns */
		float threshold = Float.parseFloat(prop
				.getProperty(Constants.APRIORI_PATTERN_REMOVAL_THRESHOLD));

		/* maximum pattern length */
		int maximumPatternLen = Integer.parseInt(prop
				.getProperty(Constants.APRIORI_MAX_PATTERN_LEN));

		if (maximumPatternLen <= 0)
			maximumPatternLen = Integer.MAX_VALUE;

		int loopCount = 0;

		TwisterMonitor monitor = null;

		int currentPatternLength = 1;

		/*
		 * mapping between pattern length and all corresponding frequent
		 * patterns
		 */
		Map<Integer, PatternSet> frequentPatterns = new HashMap<Integer, PatternSet>();

		while (!(terminated || reachedMaxPatLen)) {

			System.out.println("Checking candidate patterns of length "
					+ currentPatternLength);
			monitor = driver.runMapReduceBCast(candidatePanInput);
			monitor.monitorTillCompletion();

			PatternSet patternSet = ((AprioriCombiner) driver
					.getCurrentCombiner()).getResults();

			loopCount++;
			System.out.println("Done iteration " + loopCount);

			if (patternSet.isEmpty()) {
				System.out.println("There are no frequent patterns of length "
						+ currentPatternLength + " , going to exit");
				terminated = true;
			} else {

				// System.out.println(patternSet.toString());

				frequentPatterns.put(currentPatternLength, patternSet);

				if (currentPatternLength == 1) {
					// set frequent single event patterns
					candidatePanInput.setSingleEventPatterns(patternSet
							.getPatterns());
				}

				// set current longest frequent patterns
				candidatePanInput.setCurrentPatterns(patternSet.getPatterns());
			}

			if (currentPatternLength >= maximumPatternLen) {
				System.out.println("Reached maximum pattern length "
						+ maximumPatternLen + " , going to exit");
				reachedMaxPatLen = true;
			}

			currentPatternLength++;
		}

		// remove 'prefix' and 'included' patterns
		Map<Integer, PatternSet> cleanedPatterns = AprioriUtils
				.removePrefixAndIncluded(frequentPatterns, threshold);

		// save patterns to text file, format is similar to example file
		AprioriUtils.savePatternsToTextFile(cleanedPatterns,
				prop.getProperty(Constants.FREQUENT_PATTERN_TEXT_OUTFILE_PATH));

		// save patterns to binary file for probabilistic adjustment
		AprioriUtils.savePatternsToBinaryFile(cleanedPatterns, prop
				.getProperty(Constants.FREQUENT_PATTERN_BINARY_OUTFILE_PATH));

		// close driver
		driver.close();
	}

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

		AprioriDriver driver = null;

		try {
			driver = new AprioriDriver();
			double beginTime = System.currentTimeMillis();
			driver.driveMapReduce(partitionFile, numMapTasks, centroidFilePath,
					prop);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("Apriori-like procedure took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
