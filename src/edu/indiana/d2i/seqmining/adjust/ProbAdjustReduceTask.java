package edu.indiana.d2i.seqmining.adjust;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import edu.indiana.d2i.seqmining.ds.PatternInAdjustSet;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust.EventInAdjust;
import edu.indiana.d2i.seqmining.util.ProbAdjustUtil;

public class ProbAdjustReduceTask implements ReduceTask {

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

		try {
			PatternInAdjustSet patternsInAdjust = new PatternInAdjustSet();

			for (Value val : values) {
				PatternInAdjustSet partial = new PatternInAdjustSet(
						val.getBytes());

				mergePatternInAdjustSet(patternsInAdjust, partial);
			}

			PatternSet patterns = ProbAdjustUtil
					.patternInAdjust2Pattern(patternsInAdjust);

			collector.collect(key, new BytesValue(patterns.getBytes()));

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	private static void mergePatternInAdjustSet(PatternInAdjustSet s1,
			PatternInAdjustSet s2) {

		if (s1.getPatterns().size() == 0) {
			/* initial case when s1 is empty */
			s1.getPatterns().addAll(s2.getPatterns());
			return;
		}

		List<SeqPatternInAdjust> patternsInAdjust1 = s1.getPatterns();
		List<SeqPatternInAdjust> patternsInAdjust2 = s2.getPatterns();

		/* # of patterns in adjust should be equal */
		assert patternsInAdjust1.size() == patternsInAdjust2.size();

		for (int i = 0; i < patternsInAdjust1.size(); i++) {
			SeqPatternInAdjust p1 = patternsInAdjust1.get(i);
			SeqPatternInAdjust p2 = patternsInAdjust2.get(i);

			/* # of events should be equal */
			assert p1.getPatternInAdjust().size() == p2.getPatternInAdjust()
					.size();

			/* loop over each event */
			for (int j = 0; j < p1.getPatternInAdjust().size(); j++) {
				EventInAdjust ev1 = p1.getPatternInAdjust().get(j);
				EventInAdjust ev2 = p2.getPatternInAdjust().get(j);

				/* event type should be equal */
				assert ev1.getEventType() == ev2.getEventType();

				// merge
				ev1.setNumEvents(ev1.getNumEvents() + ev2.getNumEvents());
				ev1.setSumedStartTime(ev1.getSumedStartTime()
						+ ev2.getSumedStartTime());
				ev1.setSumedEndTime(ev1.getSumedEndTime()
						+ ev2.getSumedEndTime());
			}
		}

	}
}
