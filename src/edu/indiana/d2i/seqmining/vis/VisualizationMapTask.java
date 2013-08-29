package edu.indiana.d2i.seqmining.vis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
import cgl.imr.types.IntKey;
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.MatchProbComparator;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.PatternVis;
import edu.indiana.d2i.seqmining.ds.PatternVis.MatchedExample;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;

public class VisualizationMapTask implements MapTask {

	private FileData fileData;
	private DataSet dataset;
	// properties related with visualization
	int maxNumExDisplay;

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

		maxNumExDisplay = Integer.parseInt(jobConf
				.getProperty(Constants.VIS_MAX_NUM_MATCHED_EXAMPLES_DISPLAY));

	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// TODO Auto-generated method stub

		try {
			PatternSet patterns = new PatternSet(val.getBytes());

			/* find all event types */
			Set<Integer> allEventTypes = findAllEventsTypes(patterns);

			int idx = 0;
			for (SequentialPattern p : patterns.getPatterns()) {
				PatternVis patternVis = new PatternVis(p);

				List<MatchedExample> topExs = getTopMatchedExamples(p, dataset,
						maxNumExDisplay);

				patternVis.setMatchedExamples(topExs);

				patternVis.setAllEventTypes(allEventTypes);

				/**
				 * emit intermediate output, key is the index, value is
				 * PatternVis
				 */
				collector.collect(new IntKey(idx++),
						new BytesValue(patternVis.getBytes()));
			}

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	private static List<MatchedExample> getTopMatchedExamples(
			SequentialPattern p, DataSet dataset, int topNum) {

		List<Map.Entry<Long, Float>> entries = new ArrayList<Map.Entry<Long, Float>>(
				p.getMatchExScore().entrySet());

		Collections.sort(entries, new MatchProbComparator());

		Set<Long> ids = new HashSet<Long>();

		int numExs = Math.min(topNum, entries.size());

		for (int i = 0; i < numExs; i++) {
			ids.add(entries.get(i).getKey());
		}

		Map<Long, Example> exs = dataset.getExamplesById(ids);

		List<MatchedExample> matchedExs = new ArrayList<MatchedExample>();

		for (Long exampleId : exs.keySet()) {

			matchedExs.add(new MatchedExample(exs.get(exampleId), p
					.getMatchExScore().get(exampleId)));
		}

		return matchedExs;
	}

	private Set<Integer> findAllEventsTypes(PatternSet pset) {
		Set<Integer> allEventTypes = new HashSet<Integer>();

		for (SequentialPattern p : pset.getPatterns()) {
			allEventTypes.addAll(p.getEventTypeSet());
		}

		return allEventTypes;
	}
}
