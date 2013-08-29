package edu.indiana.d2i.seqmining.apriori;

import java.io.IOException;

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
import edu.indiana.d2i.seqmining.ds.CPatternInput;
import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;
import edu.indiana.d2i.seqmining.util.AprioriUtils;
import edu.indiana.d2i.seqmining.util.AprioriUtils.MatchResult;

public class AprioriMapTask implements MapTask {

	private static String REDUCE_KEY = "apriori-map-to-reduce-key";
	private FileData fileData;
	private DataSet dataset;
	private boolean usePenalty;
	private float penaltyThreshold;
	private float penaltyFactor;
	private float patternMatchThreshold;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		// TODO Auto-generated method stub

		usePenalty = Boolean.parseBoolean(jobConf
				.getProperty(Constants.APRIORI_EVENT_MATCH_USE_PENALTY));
		penaltyThreshold = Float.parseFloat(jobConf
				.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_THRESHOLD));
		penaltyFactor = Float.parseFloat(jobConf
				.getProperty(Constants.APRIORI_EVENT_MATCH_PENALTY_FACTOR));
		patternMatchThreshold = Float.parseFloat(jobConf
				.getProperty(Constants.APRIORI_PATTERN_MATCH_THRESHOLD));

		fileData = (FileData) mapConf.getDataPartition();

		try {
			dataset = new DataSet(fileData.getFileName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new TwisterException(e);
		}
	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// TODO Auto-generated method stub

		try {
			CPatternInput candidatePaInput = new CPatternInput(val.getBytes());

			PatternSet candidates = AprioriUtils
					.genCandidatePatterns(candidatePaInput);

			/*
			 * the following code can also deal with the case when candidates
			 * are empty
			 */

			for (SequentialPattern candidatePattern : candidates.getPatterns()) {

				for (Example example : dataset.getExamples()) {
					MatchResult res = AprioriUtils.match(example,
							candidatePattern, usePenalty, penaltyThreshold,
							penaltyFactor, patternMatchThreshold);

					if (res.isMatch()) {

						candidatePattern.addMatchedEx(example.getId(),
								res.getProb());

					}

				}
			}

			collector.collect(new StringKey(REDUCE_KEY), new BytesValue(
					candidates.getBytes()));
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

}
