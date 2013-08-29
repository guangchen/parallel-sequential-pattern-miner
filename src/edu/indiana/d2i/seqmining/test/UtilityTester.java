package edu.indiana.d2i.seqmining.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cgl.imr.base.SerializationException;

import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.CPatternInput;
import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;
import edu.indiana.d2i.seqmining.util.AprioriUtils;
import edu.indiana.d2i.seqmining.util.ClusteringUtils;
import edu.indiana.d2i.seqmining.util.AprioriUtils.MatchResult;

public class UtilityTester {

	public static void testLoadCentroidFromFile(String centroidFilePath)
			throws IOException {
		EventSpaceCentroids centroids = ClusteringUtils
				.loadFromCentroidFile(centroidFilePath);

		System.out.print(centroids);
	}

	public static void testLoadExamplesFromFile(String dataFilePath)
			throws IOException {
		DataSet dataset = new DataSet(dataFilePath);

		System.out.print(dataset);

		// test each example
		for (Example ex : dataset.getExamples()) {
			System.out.println("---example---");
			for (Map.Entry<Integer, List<Event>> entry : ex.getEventsByType()
					.entrySet()) {
				System.out.println("EventType = " + entry.getKey());
				System.out.println("---events---");

				StringBuilder sb = new StringBuilder();
				StringBuilder eventTypes = new StringBuilder();
				StringBuilder startTimes = new StringBuilder();
				StringBuilder durations = new StringBuilder();

				for (Event ev : entry.getValue()) {
					eventTypes.append(ev.getEventType() + " ");
					startTimes.append(ev.getStartTime() + " ");
					durations.append(ev.getDuration() + " ");
				}

				sb.append(eventTypes.toString() + "\n");
				sb.append(startTimes.toString() + "\n");
				sb.append(durations.toString() + "\n");

				System.out.print(sb);
				System.out.println("---events---");
			}
			System.out.println("---example---");
		}

		// test grouping events by type
		Map<Integer, List<Event>> eventGroup = ClusteringUtils
				.dataset2EventGroup(dataset.getExamples());

		StringBuilder sb = new StringBuilder();

		for (Map.Entry<Integer, List<Event>> entry : eventGroup.entrySet()) {
			sb.append(String.format("---Group (eventType=%d)---%n",
					entry.getKey()));

			StringBuilder eventTypes = new StringBuilder();
			StringBuilder startTimes = new StringBuilder();
			StringBuilder durations = new StringBuilder();

			for (Event ev : entry.getValue()) {
				eventTypes.append(ev.getEventType() + " ");
				startTimes.append(ev.getStartTime() + " ");
				durations.append(ev.getDuration() + " ");
			}

			sb.append(eventTypes.toString() + "\n");
			sb.append(startTimes.toString() + "\n");
			sb.append(durations.toString() + "\n");

			sb.append("---Group---\n");
		}

		System.out.println("Grouping events by event type");
		System.out.println(sb.toString());

	}

	public static void testLoadPropertyFile(String propFilePath)
			throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		prop.load(new FileInputStream(propFilePath));

		System.out.print(String.format("%s=%s%n",
				Constants.CLUSTERING_MAX_NUM_ITER,
				prop.getProperty(Constants.CLUSTERING_MAX_NUM_ITER)));

		System.out.print(String.format("%s=%s%n",
				Constants.CLUSTERING_CONVERGE_THRESHOLD,
				prop.getProperty(Constants.CLUSTERING_CONVERGE_THRESHOLD)));

		System.out.print(String.format("%s=%s%n",
				Constants.DATASET_NUM_EXAMPLES,
				prop.getProperty(Constants.DATASET_NUM_EXAMPLES)));

		System.out.print(String.format("%s=%s%n",
				Constants.APRIORI_MIN_SUP_PROB,
				prop.getProperty(Constants.APRIORI_MIN_SUP_PROB)));

		System.out.print(String.format("%s=%s%n",
				Constants.APRIORI_MAX_PATTERN_LEN,
				prop.getProperty(Constants.APRIORI_MAX_PATTERN_LEN)));

		System.out
				.print(String.format(
						"%s=%s%n",
						Constants.EVENT_MATCHER_OVERLAP_LENGTH_RATIO,
						prop.getProperty(Constants.EVENT_MATCHER_OVERLAP_LENGTH_RATIO)));
	}

	public static void testLoadConvergedPatterns(String centroidFilePath)
			throws IOException {
		CPatternInput candidatePanInput = AprioriUtils
				.loadPatternSetFromFile(centroidFilePath);

		System.out.print(candidatePanInput);
	}

	public static void testPatternMatch(String dataFilePath,
			String centroidFilePath, String propFilePath) throws IOException {

		Properties prop = new Properties();
		prop.load(new FileInputStream(propFilePath));

		DataSet dataset = new DataSet(dataFilePath);

		CPatternInput candidatePanInput = AprioriUtils
				.loadPatternSetFromFile(centroidFilePath);

		PatternSet candidates = AprioriUtils
				.genCandidatePatterns(candidatePanInput);

		boolean usePenalty = Boolean.parseBoolean(prop
				.getProperty(Constants.APRIORI_EVENT_MATCH_USE_PENALTY));

		float penaltyThreshold = Float.parseFloat(prop
				.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_THRESHOLD));

		float penaltyFactor = Float.parseFloat(prop
				.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_FACTOR));

		float patternMatchThreshold = Float.parseFloat(prop
				.getProperty(Constants.APRIORI_PATTERN_MATCH_THRESHOLD));

		for (SequentialPattern candidatePattern : candidates.getPatterns()) {

			for (Example example : dataset.getExamples()) {
				System.out.println("---begin match---");
				System.out.println(example.toString());
				System.out.println(candidatePattern.toString());

				MatchResult res = AprioriUtils.match(example, candidatePattern,
						usePenalty, penaltyThreshold, penaltyFactor,
						patternMatchThreshold);

				if (res.isMatch()) {
					candidatePattern.addMatchedEx(example.getId(),
							res.getProb());
					System.out.println("A match");
				} else {
					System.out.println("Not a match");
				}
				System.out.println("---end match---");
			}
		}
	}

	public static void testInitialValue() {
		float[] floatArray = new float[3];

		for (int i = 0; i < floatArray.length; i++) {
			System.out.print(floatArray[i] + " ");
		}

		System.out.println("\n");
	}

	public static void testNthRoot() {
		double base = 0.7 * 0.7 * 0.7;
		double exponent = 1.0 / 3;

		System.out.printf("pow(%f, %f) = %f%n", base, exponent,
				Math.pow(base, exponent));

	}

	public static void testScientificNotation() {
		String number = "1e05";

		System.out.printf("Number = %d%n", Double.valueOf(number).longValue());
	}

	public static void testLoadPatternBinary(String patternBinaryFilePath)
			throws IOException, SerializationException {
		PatternSet patterns = AprioriUtils
				.loadPatternsFromBinaryFile(patternBinaryFilePath);
		
//		for (SequentialPattern p : patterns.getPatterns()) {
//			System.out.println(p);
//		}
		
		SequentialPattern p = patterns.getPatterns().get(0);
		System.out.println(p);
		
		for (Long exampleId : p.getMatchedExampleId()) {
			System.out.printf("Example id = %d%n", exampleId);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws SerializationException 
	 */
	public static void main(String[] args) throws IOException, SerializationException {
		// TODO Auto-generated method stub

		// String centroidFilePath = "data\\centroids_seed.txt";
		//
		// testLoadCentroidFromFile(centroidFilePath);

		String dataFilePath = "data\\example-data-3.txt";

		// testLoadExamplesFromFile(dataFilePath);

		String propFilePath = "conf\\seqmining.properties";
		//
		testLoadPropertyFile(propFilePath);

		// testInitialValue();

		String convergedCentroidFilePath = "data\\converged_centroids";
		// testLoadConvergedPatterns(convergedCentroidFilePath);

//		testPatternMatch(dataFilePath, convergedCentroidFilePath, propFilePath);
//
//		testNthRoot();
//
//		testScientificNotation();
		
		String patternBinaryFilePath = "data\\frequent_patterns_binary";
		testLoadPatternBinary(patternBinaryFilePath);
	}

}
