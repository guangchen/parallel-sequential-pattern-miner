package edu.indiana.d2i.seqmining.apriori;

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
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;

public class AprioriReduceTask implements ReduceTask {

	private float minSupportProp;
	private long numTotalExamples;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
		// TODO Auto-generated method stub

		minSupportProp = Float.parseFloat(jobConf
				.getProperty(Constants.APRIORI_MIN_SUP_PROB));

		numTotalExamples = Long.parseLong(jobConf
				.getProperty(Constants.DATASET_NUM_EXAMPLES));
	}

	@Override
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		// TODO Auto-generated method stub

		try {
			PatternSet candidates = new PatternSet();

			for (Value v : values) {
				PatternSet patternSet = new PatternSet(v.getBytes());

				megerPatternSet(candidates, patternSet);
			}

			// check minimum support, filter out less frequent candidates

			PatternSet frequentPatterns = new PatternSet();

			for (SequentialPattern p : candidates.getPatterns()) {
				if ((p.getTotalScore() / (float) numTotalExamples) > minSupportProp) {
					frequentPatterns.addPattern(p);
				}

			}

			collector.collect(key, new BytesValue(frequentPatterns.getBytes()));

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	private static void megerPatternSet(PatternSet p1, PatternSet p2) {
		if (p1.getPatterns().size() == 0) {
			/* the initial case when p1 is empty */
			p1.getPatterns().addAll(p2.getPatterns());
			return;
		}

		List<SequentialPattern> candidates1 = p1.getPatterns();
		List<SequentialPattern> candidates2 = p2.getPatterns();

		/* # of candidates should be equal */
		assert candidates1.size() == candidates2.size();

		for (int i = 0; i < candidates1.size(); i++) {
			SequentialPattern pattern1 = candidates1.get(i);

			Map<Long, Float> scoreTable1 = candidates1.get(i).getMatchExScore();
			Map<Long, Float> scoreTable2 = candidates2.get(i).getMatchExScore();

			for (Map.Entry<Long, Float> entry : scoreTable2.entrySet()) {
				assert scoreTable1.containsKey(entry.getKey()) == false;

				/**
				 * Note, we can not use
				 * "scoreTable1.put(entry.getKey(), entry.getValue());" to
				 * update the score table directly since that way doesn't update
				 * the variable "totalScore"
				 */
				pattern1.addMatchedEx(entry.getKey(), entry.getValue());
			}
		}
	}
}
