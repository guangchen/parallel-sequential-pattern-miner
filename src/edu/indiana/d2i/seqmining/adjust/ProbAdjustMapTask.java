package edu.indiana.d2i.seqmining.adjust;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.OverlapEventMatcher;
import edu.indiana.d2i.seqmining.ds.PatternInAdjustSet;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust.EventInAdjust;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;

public class ProbAdjustMapTask implements MapTask {

	private static String REDUCE_KEY = "prob-adjust-map-to-reduce-key";
	private FileData fileData;
	private DataSet dataset;
	private float lengthRatio;

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

		lengthRatio = Float.parseFloat(jobConf
				.getProperty(Constants.EVENT_MATCHER_OVERLAP_LENGTH_RATIO));
	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// TODO Auto-generated method stub

		try {
			OverlapEventMatcher overlapMatcher = new OverlapEventMatcher(
					lengthRatio);
			PatternInAdjustSet patternInAdjustSet = new PatternInAdjustSet();
			PatternSet patterns = new PatternSet(val.getBytes());

			/**
			 * do probabilistic adjustment for each pattern
			 */
			for (SequentialPattern pattern : patterns.getPatterns()) {
				// sort the pattern
				pattern.getPattern(true);

				// get all event types that the pattern has
				Set<Integer> eventTypes = pattern.getEventTypeSet();

				/*
				 * build mapping table between event type and corresponding
				 * events
				 */

				// initialize the mapping table
				Map<Integer, List<Event>> eventsByType = new HashMap<Integer, List<Event>>();
				for (Integer eventType : eventTypes) {
					eventsByType.put(eventType, new ArrayList<Event>());
				}

				Set<Long> matchedExampleId = pattern.getMatchedExampleId();

				// loop over each example to build the mapping table
				for (Example ex : dataset.getExamples()) {

					if (!matchedExampleId.contains(ex.getId())) {
						// not a matched example, just continue
						continue;
					}

					for (Map.Entry<Integer, List<Event>> entry : eventsByType
							.entrySet()) {

						entry.getValue()
								.addAll(ex.getEventsByType(entry.getKey(),
										false, null));
					}
				}

				// conduct probabilistic adjustment
				SeqPatternInAdjust patternInAdjust = probAdjust(pattern,
						eventsByType, overlapMatcher);

				patternInAdjustSet.addPattern(patternInAdjust);
			}

			collector.collect(new StringKey(REDUCE_KEY), new BytesValue(
					patternInAdjustSet.getBytes()));
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	private static SeqPatternInAdjust probAdjust(SequentialPattern pattern,
			Map<Integer, List<Event>> eventsByType, OverlapEventMatcher matcher) {

		SeqPatternInAdjust patternInAdjust = new SeqPatternInAdjust();

		for (Event ev : pattern.getPattern(true)) {
			List<Event> events = eventsByType.get(ev.getEventType());

			List<Event> matchedEvents = findMatchedEvent(ev, events, matcher);

			float summedStartTime = 0f;
			float summedEndTime = 0f;

			for (Event matcheEvent : matchedEvents) {
				summedStartTime += matcheEvent.getStartTime();
				summedEndTime += matcheEvent.getEndTime();
			}

			patternInAdjust.addEvent(new EventInAdjust(ev.getEventType(),
					summedStartTime, summedEndTime, matchedEvents.size()));
		}

		return patternInAdjust;
	}

	private static List<Event> findMatchedEvent(Event ev, List<Event> events,
			OverlapEventMatcher matcher) {
		List<Event> matchedEvent = new ArrayList<Event>();

		for (Event event : events) {

			if (matcher.match(ev, event)) {
				matchedEvent.add(event);
			}

		}

		return matchedEvent;
	}

}
