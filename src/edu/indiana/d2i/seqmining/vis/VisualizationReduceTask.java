package edu.indiana.d2i.seqmining.vis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.mathworks.toolbox.javabuilder.MWCellArray;
import com.mathworks.toolbox.javabuilder.MWCharArray;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.IntKey;
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.FigureSet;
import edu.indiana.d2i.seqmining.ds.PatternQueryFigure;
import edu.indiana.d2i.seqmining.ds.PatternVis;
import edu.indiana.d2i.seqmining.ds.PatternVis.MatchedExample;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;
import edu.indiana.matlab.pattern.GenPatternQueryImgs;

public class VisualizationReduceTask implements ReduceTask {
	/**
	 * number of lines spanned by an example/pattern, it is 3 lines: event
	 * types, start times and durations
	 */
	private static int NUM_LINES_SPANNED_EX = 3;
	private String figureOutputDir;
	private String figureFormat;
	private int numBins;
	private int maxNumExPerPage;

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
		// TODO Auto-generated method stub

		numBins = Integer.parseInt(jobConf
				.getProperty(Constants.VIS_FIGURE_HISTO_NUM_BIN));

		maxNumExPerPage = Integer.parseInt(jobConf
				.getProperty(Constants.VIS_FIGURE_MAX_NUM_EX_PER_PAGE));

		figureOutputDir = jobConf
				.getProperty(Constants.VIS_FIGURE_REDUCE_OUTPUT_DIR);

		figureFormat = jobConf.getProperty(Constants.VIS_FIGURE_FORMAT);
	}

	@Override
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		// TODO Auto-generated method stub

		try {
			PatternVis patternVis = new PatternVis();
			for (Value val : values) {
				PatternVis partial = new PatternVis(val.getBytes());

				mergePatternVis(patternVis, partial);
			}

			// invoke matlab visualization code

			GenPatternQueryImgs queryImgGenertor = null;

			/* prepare input arguments */

			/* pattern id */
			MWNumericArray patternIdScalar = new MWNumericArray(
					((IntKey) key).getKey());

			/* pattern */
			SequentialPattern seqPattern = patternVis.getPattern();
			List<Event> events = seqPattern.getPattern(false);

			double[] patternArray = new double[NUM_LINES_SPANNED_EX
					* events.size()];
			int idx = 0;

			/* fill array in column major order */
			for (int i = 0; i < events.size(); i++) {
				Event e = events.get(i);

				patternArray[idx++] = e.getEventType();
				patternArray[idx++] = e.getStartTime();
				patternArray[idx++] = e.getDuration();
			}

			/**
			 * dimension of the matrix is NUM_LINES_SPANNED_EX * N where N is
			 * the number of events of the pattern
			 */
			int[] dims = { NUM_LINES_SPANNED_EX, events.size() };

			MWNumericArray patternMatrix = MWNumericArray.newInstance(dims,
					patternArray, MWClassID.DOUBLE);

			/* all probabilities */
			Collection<Float> probs = seqPattern.getMatchExScore().values();
			double[] probArray = new double[probs.size()];

			/* fill vector */
			idx = 0;
			for (Float probValue : probs) {
				probArray[idx++] = probValue;
			}

			// column vector
			dims[0] = probArray.length;
			dims[1] = 1;

			MWNumericArray allProbVector = MWNumericArray.newInstance(dims,
					probArray, MWClassID.DOUBLE);

			/* matched probabilities */
			double[] matchedProbArray = new double[patternVis
					.getMatchedExamples().size()];

			/* matched examples */
			dims[0] = patternVis.getMatchedExamples().size();
			dims[1] = 1;

			MWCellArray matchedExamples = new MWCellArray(dims);

			/* fill vector */
			idx = 0;
			for (MatchedExample ex : patternVis.getMatchedExamples()) {
				matchedProbArray[idx++] = ex.getMatchingProb();

				/**
				 * note that index in matlab is 1-based
				 */
				matchedExamples.set(idx,
						genNWNumericArrayFromExample(ex.getExample()));
			}

			// column vector
			dims[0] = matchedProbArray.length;
			dims[1] = 1;

			MWNumericArray matchedProbVector = MWNumericArray.newInstance(dims,
					matchedProbArray, MWClassID.DOUBLE);

			/* all event types */
			List<Integer> allEventTypes = new ArrayList<Integer>();
			allEventTypes.addAll(patternVis.getAllEventTypes());
			/**
			 * we need to sort here so that all patterns have the same order for
			 * event types
			 */
			Collections.sort(allEventTypes);

			int[] typeArray = new int[allEventTypes.size()];
			idx = 0;
			for (Integer type : allEventTypes) {
				typeArray[idx++] = type;
			}

			// column vector
			dims[0] = typeArray.length;
			dims[1] = 1;

			MWNumericArray allEventTypesVector = MWNumericArray.newInstance(
					dims, typeArray, MWClassID.INT32);

			/* max number of examples displayed per page */
			MWNumericArray maxNumExPerPageScalar = new MWNumericArray(
					maxNumExPerPage);

			/* number bins of histogram */
			/**
			 * note that we need cast the numBins to double as required by
			 * Matlab hist() function
			 */
			MWNumericArray numBinsScalar = new MWNumericArray((double) numBins);

			try {
				queryImgGenertor = new GenPatternQueryImgs();

				/* call the function, first argument is nargout */
				Object[] result = queryImgGenertor.gen_query_imgs(1,
						patternIdScalar, patternMatrix, matchedExamples,
						allProbVector, matchedProbVector, allEventTypesVector,
						maxNumExPerPageScalar, numBinsScalar, figureOutputDir,
						figureFormat);

				MWCellArray figureOutputPaths = (MWCellArray) result[0];

				List<PatternQueryFigure> figures = new ArrayList<PatternQueryFigure>(
						figureOutputPaths.numberOfElements());

				for (int i = 0; i < figureOutputPaths.numberOfElements(); i++) {
					/* matlab index is 1-based */
					MWCharArray figurePath = new MWCharArray(
							figureOutputPaths.get(i + 1));

					String figurePathStr = figurePath.toString();
					byte[] figureContent = FileUtils
							.readFileToByteArray(new File(figurePathStr));

					String figureName = null;
					int ind = figurePathStr.lastIndexOf(File.separator);

					if (ind == -1) {
						/* figure is written to current working directory */
						figureName = figurePathStr;
					} else {
						figureName = figurePathStr.substring(ind + 1);
					}

					figures.add(new PatternQueryFigure(figureName,
							figureContent));
				}

				FigureSet figureSet = new FigureSet();
				figureSet.setFigures(figures);

				collector.collect(key, new BytesValue(figureSet.getBytes()));

			} catch (MWException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (queryImgGenertor != null) {
					queryImgGenertor.dispose();
				}

				matchedExamples.dispose();
				MWNumericArray.disposeArray(patternIdScalar);
				MWNumericArray.disposeArray(patternMatrix);
				MWNumericArray.disposeArray(allProbVector);
				MWNumericArray.disposeArray(matchedProbVector);
				MWNumericArray.disposeArray(allEventTypesVector);
				MWNumericArray.disposeArray(maxNumExPerPageScalar);
				MWNumericArray.disposeArray(numBinsScalar);
			}
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	private void mergePatternVis(PatternVis p1, PatternVis p2) {
		/* initial case that p1 is empty */
		if (p1.getPattern() == null) {
			p1.setPattern(p2.getPattern());
			p1.setMatchedExamples(p2.getMatchedExamples());
			p1.setAllEventTypes(p2.getAllEventTypes());

			return;
		}

		// merge
		p1.getMatchedExamples().addAll(p2.getMatchedExamples());
	}

	private static MWNumericArray genNWNumericArrayFromExample(Example ex) {
		List<Event> events = ex.getEvents();

		double[] exampleArray = new double[NUM_LINES_SPANNED_EX * events.size()];
		int idx = 0;

		/* fill array in column major order */
		for (int i = 0; i < events.size(); i++) {
			Event e = events.get(i);

			exampleArray[idx++] = e.getEventType();
			exampleArray[idx++] = e.getStartTime();
			exampleArray[idx++] = e.getDuration();
		}

		int[] dims = { NUM_LINES_SPANNED_EX, events.size() };

		return MWNumericArray.newInstance(dims, exampleArray, MWClassID.DOUBLE);
	}
}
