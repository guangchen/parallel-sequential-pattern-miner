package edu.indiana.d2i.seqmining.vis;

import cgl.imr.base.Key;
import cgl.imr.base.ReducerSelector;
import cgl.imr.types.IntKey;

public class VisualizationReducerSelector extends ReducerSelector {

	public VisualizationReducerSelector() {
		super();
	}

	public VisualizationReducerSelector(int numReducers, String sinkBase,
			String jobId) {
		super(numReducers, sinkBase, jobId);
	}

	@Override
	public int getReducerNumber(Key key) {
		IntKey k = (IntKey) key;
		return Math.abs(k.getKey().intValue()) % numReducers;
	}

}
