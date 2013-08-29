package edu.indiana.d2i.seqmining.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cgl.imr.base.SerializationException;

import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.Centroids;
import edu.indiana.d2i.seqmining.ds.EventSpaceCentroids.ClusterCenter;
import edu.indiana.d2i.seqmining.ds.CPatternInput;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;

public class AprioriUtils {
	private static float THRESHOLD = 1.0E-5f;

	public static class MatchResult {
		private boolean match;
		private float prob;

		public MatchResult(boolean match, float prob) {
			super();
			this.match = match;
			this.prob = prob;
		}

		public boolean isMatch() {
			return match;
		}

		public float getProb() {
			return prob;
		}

	}

	public static CPatternInput loadPatternSetFromFile(
			String convergedCentroidFilePath) throws IOException {
		CPatternInput patternSet = new CPatternInput();

		EventSpaceCentroids centroids = ClusteringUtils
				.loadFromCentroidFile(convergedCentroidFilePath);

		List<SequentialPattern> singleEventPatterns = new ArrayList<SequentialPattern>();

		int counter = 0;
		int patternLength = 1;
		for (Map.Entry<Integer, Centroids> entry : centroids.getAllCentroids()
				.entrySet()) {

			for (ClusterCenter center : entry.getValue().getCentroids()) {
				StringBuilder patternId = new StringBuilder();
				patternId.append("candidate-[").append(patternLength)
						.append("]-[").append(counter++).append("]");

				SequentialPattern pattern = new SequentialPattern(
						patternId.toString());

				pattern.addEvent(new Event(entry.getKey(), center.getCenter()
						.getXcoord(), center.getCenter().getYcoord()));

				singleEventPatterns.add(pattern);
			}
		}

		/**
		 * should leave singleEventPatterns empty to indicate the first
		 * iteration
		 */
		patternSet.setCurrentPatterns(singleEventPatterns);
		return patternSet;
	}

	public static PatternSet genCandidatePatterns(CPatternInput input) {
		PatternSet candidates = new PatternSet();

		if (input.getSingleEventPatterns().size() == 0) {
			// it's the case of length-1 candidate patterns
			candidates.setPatterns(input.getCurrentPatterns());
			return candidates;
		}

		// regular cases
		for (SequentialPattern currentPattern : input.getCurrentPatterns()) {
			// sort it by start time
			List<Event> events = currentPattern.getPattern(true);
			Event lastEvent = events.get(events.size() - 1);

			for (SequentialPattern singleEventPattern : input
					.getSingleEventPatterns()) {

				Event singleEvent = singleEventPattern.getPattern(false).get(0);

				/**
				 * two conditions to expand the pattern (1) single event pattern
				 * is the absolutely latest one, (2) same start time but of
				 * different event types
				 */
				if ((singleEvent.getStartTime() > lastEvent.getStartTime())
						|| ((Math.abs(singleEvent.getStartTime()
								- lastEvent.getStartTime()) < THRESHOLD) && (lastEvent
								.getEventType() != singleEvent.getEventType()))) {

					SequentialPattern pattern = new SequentialPattern();

					pattern.getPattern(false).addAll(events);
					pattern.getPattern(false).add(singleEvent);

					candidates.addPattern(pattern);
				}
			}
		}

		return candidates;
	}

	/**
	 * Match between an example and a pattern
	 * 
	 * @param example
	 * @param pattern
	 * @param usePenalty
	 * @param penaltyThreshold
	 * @param penaltyFactor
	 * @return
	 */
	public static MatchResult match(Example example, SequentialPattern pattern,
			boolean usePenalty, float penaltyThreshold, float penaltyFactor,
			float patternMatchThreshold) {

		float prob = 1.0f;

		if (pattern.getNumEvents() > example.getNumEvents()) {
			return new MatchResult(false, 0f);
		}

		for (Event event : pattern.getPattern(false)) {
			MatchResult matchRes = match(example, event, usePenalty,
					penaltyThreshold, penaltyFactor);

			if (!matchRes.isMatch()) {
				return new MatchResult(false, 0f);
			}

			prob *= matchRes.getProb();
		}

		// normalize the score by pattern length
		prob = (float) Math.pow(prob, 1.0 / pattern.getNumEvents());

		boolean match = false;
		/**
		 * a match if the similarity score is greater than the specified
		 * threshold
		 */
		if (prob >= patternMatchThreshold) {
			match = true;
		}

		return new MatchResult(match, prob);
	}

	/**
	 * Match between an example and an event
	 * 
	 * @param example
	 * @param event
	 * @param usePenalty
	 * @param penaltyThreshold
	 * @param penaltyFactor
	 * @return
	 */
	public static MatchResult match(Example example, Event event,
			boolean usePenalty, float penaltyThreshold, float penaltyFactor) {

		List<Event> events = example.getEventsByType(event.getEventType(),
				true, null);

		if ((null == events) || (events.size() == 0)) {
			return new MatchResult(false, 0f);
		}

		List<Event> overlapped = new ArrayList<Event>();

		for (Event ev : events) {
			float maxStartTime = Math.max(ev.getStartTime(),
					event.getStartTime());
			float minEndTime = Math.min(ev.getEndTime(), event.getEndTime());

			if (!(minEndTime > maxStartTime)) {
				// no overlap
				continue;
			}

			overlapped.add(ev);
		}

		if (overlapped.size() == 0) {
			return new MatchResult(false, 0f);
		}

		float penalty = 1.0f;

		// XOR style matching
		float xorRange = Math.abs(overlapped.get(0).getStartTime()
				- event.getStartTime());

		if (usePenalty && ((xorRange / event.getDuration()) > penaltyThreshold))
			penalty *= penaltyFactor;

		float diff = 0f;
		for (int i = 1; i < overlapped.size(); i++) {
			diff = overlapped.get(i).getStartTime()
					- overlapped.get(i - 1).getEndTime();
			xorRange += diff;

			if (usePenalty && ((diff / event.getDuration()) > penaltyThreshold))
				penalty *= penaltyFactor;
		}

		diff = Math.abs(overlapped.get(overlapped.size() - 1).getEndTime()
				- event.getEndTime());
		xorRange += diff;

		if (usePenalty && ((diff / event.getDuration()) > penaltyThreshold))
			penalty *= penaltyFactor;

		if (usePenalty) {
			return new MatchResult(true, (example.getInterval() - xorRange)
					/ example.getInterval() / penalty);
		} else {
			return new MatchResult(true, (example.getInterval() - xorRange)
					/ example.getInterval());
		}
	}

	public static void savePatternsToTextFile(PatternSet frequentPatterns,
			String outputFilePath) throws IOException {

		BufferedWriter writer = null;

		try {

			// count how many patterns
			int count = 0;
			StringBuilder content = new StringBuilder();

			for (SequentialPattern pattern : frequentPatterns.getPatterns()) {

				// pattern id
				content.append(count++).append("\n");
				StringBuilder eventTypes = new StringBuilder();
				StringBuilder startTimes = new StringBuilder();
				StringBuilder durations = new StringBuilder();

				for (Event ev : pattern.getPattern(true)) {
					eventTypes.append(ev.getEventType()).append(" ");
					startTimes.append(ev.getStartTime()).append(" ");
					durations.append(ev.getDuration()).append(" ");
				}

				eventTypes.append("\n");
				startTimes.append("\n");
				durations.append("\n");

				content.append(eventTypes.toString());
				content.append(startTimes.toString());
				content.append(durations.toString());
			}

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputFilePath)));

			writer.write(count + "\n");
			writer.write(content.toString());
			writer.flush();
		} finally {
			if (writer != null)
				writer.close();
		}
	}

	public static void savePatternsToTextFile(
			Map<Integer, PatternSet> frequentPatterns, String outputFilePath)
			throws IOException {

		PatternSet patterns = new PatternSet();

		for (PatternSet patternSet : frequentPatterns.values()) {
			patterns.getPatterns().addAll(patternSet.getPatterns());
		}

		savePatternsToTextFile(patterns, outputFilePath);

	}

	public static void savePatternsToBinaryFile(PatternSet frequentPatterns,
			String outputFilePath) throws IOException, SerializationException {

		DataOutputStream dout = null;

		try {
			dout = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(outputFilePath)));

			byte[] bytes = frequentPatterns.getBytes();

			// write number of bytes
			dout.writeInt(bytes.length);

			// write content
			dout.write(bytes);

			dout.flush();
		} finally {
			if (dout != null)
				dout.close();
		}
	}

	public static void savePatternsToBinaryFile(
			Map<Integer, PatternSet> frequentPatterns, String outputFilePath)
			throws IOException, SerializationException {
		PatternSet patterns = new PatternSet();

		for (PatternSet patternSet : frequentPatterns.values()) {
			patterns.getPatterns().addAll(patternSet.getPatterns());
		}

		savePatternsToBinaryFile(patterns, outputFilePath);
	}

	public static PatternSet loadPatternsFromBinaryFile(String fileName)
			throws IOException, SerializationException {

		DataInputStream din = null;

		try {
			din = new DataInputStream(new BufferedInputStream(
					new FileInputStream(fileName)));

			int numBytes = din.readInt();
			byte[] bytes = new byte[numBytes];

			int numBytesRead = din.read(bytes);
			assert numBytesRead == numBytes;

			PatternSet patternSet = new PatternSet(bytes);
			return patternSet;
		} finally {
			if (din != null)
				din.close();
		}
	}

	public static float getTotalScore(Collection<Float> scores) {
		float totalScore = 0f;

		for (Float s : scores) {
			totalScore += s;
		}

		return totalScore;
	}

	/**
	 * Check if pattern 'a' is a prefix of pattern 'b'
	 * 
	 * @param a
	 *            pattern a
	 * @param b
	 *            pattern b
	 * @param threshold
	 *            threshold that determines whether two events are considered to
	 *            be close enough
	 * @return true if pattern a is a prefix of pattern b, false otherwise
	 */
	public static boolean isPrefix(SequentialPattern a, SequentialPattern b,
			float threshold) {

		/* a is longer then b */
		if (a.getNumEvents() >= b.getNumEvents())
			return false;

		List<Event> eventsA = a.getPattern(true);
		List<Event> eventsB = b.getPattern(true).subList(0, eventsA.size());

		/* event types must be equal and events should be close enough */
		for (int i = 0; i < eventsA.size(); i++) {
			Event ea = eventsA.get(i);
			Event eb = eventsB.get(i);

			if (ea.getEventType() != eb.getEventType()) {
				return false;
			}

			if ((Math.abs(ea.getStartTime() - eb.getStartTime()) > threshold)
					|| (Math.abs(ea.getDuration() - eb.getDuration()) > threshold)) {
				return false;
			}

		}

		return true;
	}

	/**
	 * function that checks whether pattern 'eventsA' is included in 'eventsB'.
	 * Note that inclusion of 'a' needs not be consecutive, i.e. 'a' needs not
	 * be a consecutive part in 'b' but events in 'a' need keep the same order
	 * 
	 * @param eventsA
	 * @param eventsB
	 * @param threshold
	 * @return
	 * 
	 *         N.B. 'eventsA' and 'eventsB' should be sorted before calling this
	 *         function
	 */
	public static boolean isIncluded(List<Event> eventsA, List<Event> eventsB,
			float threshold) {

		if (eventsA.size() > eventsB.size())
			return false;

		Event firstEvInA = eventsA.get(0);

		/*
		 * events in 'eventsB' that have the same event type as the first event
		 * of pattern 'eventsA'
		 */
		List<Event> evs = new ArrayList<Event>();
		for (Event e : eventsB) {
			if (e.getEventType() == firstEvInA.getEventType()) {
				evs.add(e);
			}
		}

		/* no events in 'eventsB' of the same event type */
		if (evs.size() == 0) {
			return false;
		}

		for (int i = 0; i < evs.size(); i++) {
			Event e = evs.get(i);

			/* not close enough */
			if ((Math.abs(e.getStartTime() - firstEvInA.getStartTime()) > threshold)
					|| (Math.abs(e.getDuration() - firstEvInA.getDuration()) > threshold)) {
				continue;
			}

			/* no more events in pattern 'eventsA' */
			if (eventsA.size() == 1) {
				return true;
			}

			/* no more events in pattern 'eventsB' */
			if (eventsB.size() == (i + 1)) {
				return false;
			}

			/* recursive */
			if (isIncluded(eventsA.subList(1, eventsA.size()),
					eventsB.subList(i + 1, eventsB.size()), threshold)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 
	 * @param frequentPatterns
	 * @param threshold
	 * @return
	 */
	public static Map<Integer, PatternSet> removePrefixAndIncluded(
			Map<Integer, PatternSet> frequentPatterns, float threshold) {

		/* sorted by pattern length */
		Map<Integer, PatternSet> sortedMap = new TreeMap<Integer, PatternSet>(
				frequentPatterns);

		PatternSet patterns = new PatternSet();

		for (PatternSet patternSet : sortedMap.values()) {
			patterns.getPatterns().addAll(patternSet.getPatterns());
		}

		/*
		 * remaining patterns after filtering out 'prefix' and 'included'
		 * patterns
		 */
		PatternSet remainingPatterns = new PatternSet();
		int numPatterns = patterns.getPatterns().size();

		for (int i = 0; i < numPatterns; i++) {

			boolean exclude = false;

			/**
			 * note that patterns are already sorted in ascending order by
			 * pattern length, so we only need to check subsequent patterns
			 */
			for (int j = i + 1; j < numPatterns; j++) {
				if (isPrefix(patterns.getPatterns().get(i), patterns
						.getPatterns().get(j), threshold)
						|| isIncluded(
								patterns.getPatterns().get(i).getPattern(true),
								patterns.getPatterns().get(j).getPattern(true),
								threshold)) {
					exclude = true;
					break;
				}
			}

			if (!exclude) {
				remainingPatterns.getPatterns().add(
						patterns.getPatterns().get(i));
			}
		}

		Map<Integer, PatternSet> res = new TreeMap<Integer, PatternSet>();

		for (SequentialPattern p : remainingPatterns.getPatterns()) {

			PatternSet pSet = res.get(p.getNumEvents());
			if (pSet == null) {
				pSet = new PatternSet();
				res.put(p.getNumEvents(), pSet);
			}

			pSet.addPattern(p);
		}

		return res;
	}
}
