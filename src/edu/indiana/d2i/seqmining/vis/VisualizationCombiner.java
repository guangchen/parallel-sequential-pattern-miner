package edu.indiana.d2i.seqmining.vis;

import java.util.Map;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import edu.indiana.d2i.seqmining.ds.FigureSet;

public class VisualizationCombiner implements Combiner {

	private FigureSet figureSet;

	@Override
	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		// TODO Auto-generated method stub

		try {

			for (Map.Entry<Key, Value> entry : keyValues.entrySet()) {
				FigureSet fset = new FigureSet(entry.getValue().getBytes());

				figureSet.getFigures().addAll(fset.getFigures());
			}

		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	@Override
	public void configure(JobConf jobConf) throws TwisterException {
		// TODO Auto-generated method stub

		figureSet = new FigureSet();
	}

	public FigureSet getResults() {
		return figureSet;
	}
}
