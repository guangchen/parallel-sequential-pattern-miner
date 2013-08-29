package edu.indiana.d2i.seqmining.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.mathworks.toolbox.javabuilder.MWCellArray;
import com.mathworks.toolbox.javabuilder.MWCharArray;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;

import edu.indiana.d2i.seqmining.ds.DataSet;
import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.Example;
import edu.indiana.d2i.seqmining.ds.FigureSet;
import edu.indiana.d2i.seqmining.ds.PatternQueryFigure;
import edu.indiana.d2i.seqmining.util.VisualizationUtil;
import edu.indiana.matlab.pattern.GenPatternQueryImgs;

public class VisTester {
	private static int NUM_LINES_SPANNED_EX = 3;

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

	public static void testVisFunction(String dataFilePath, int numTotalEvents,
			int numTotalEx, int maxNumExPerPage, int numBins,
			String figureOutputDir, String figureFormat, String finalOutputDir)
			throws IOException, MWException {

		DataSet dataset = new DataSet(dataFilePath);

		if (dataset.getExamples().size() < 2) {
			System.out
					.println("# of examples in the data set is < 2, just return");
			return;
		}

		Random rnd = new Random();

		List<Example> examples = dataset.getExamples();

		// first example used as pattern
		MWNumericArray patternIdScalar = new MWNumericArray(examples.get(0)
				.getId());

		MWNumericArray patternArray = genNWNumericArrayFromExample(examples
				.get(0));

		// matched examples
		int[] dims = { examples.size() - 1, 1 };

		MWCellArray matchedExamples = new MWCellArray(dims);

		for (int i = 1; i < examples.size(); i++) {
			/**
			 * index in matlab is 1-based
			 */
			matchedExamples.set(i,
					genNWNumericArrayFromExample(examples.get(i)));
		}

		/* all probabilities */
		double[] probArray = new double[numTotalEx];

		/* fill vector */
		for (int i = 0; i < probArray.length; i++) {
			probArray[i] = rnd.nextDouble();
		}

		// column vector
		dims[0] = probArray.length;
		dims[1] = 1;

		MWNumericArray allProbVector = MWNumericArray.newInstance(dims,
				probArray, MWClassID.DOUBLE);

		/* matched probabilities */
		double[] matchedProbArray = new double[examples.size() - 1];

		/* matched examples */
		dims[0] = examples.size() - 1;
		dims[1] = 1;

		for (int i = 0; i < matchedProbArray.length; i++) {
			matchedProbArray[i] = rnd.nextDouble();
		}

		MWNumericArray matchedProbVector = MWNumericArray.newInstance(dims,
				matchedProbArray, MWClassID.DOUBLE);

		/* all event types */
		int[] typeArray = new int[numTotalEvents];

		for (int i = 0; i < typeArray.length; i++) {
			typeArray[i] = i + 1;
		}

		// column vector
		dims[0] = typeArray.length;
		dims[1] = 1;

		MWNumericArray allEventTypesVector = MWNumericArray.newInstance(dims,
				typeArray, MWClassID.INT32);

		/* max number of examples displayed per page */
		MWNumericArray maxNumExPerPageScalar = new MWNumericArray(
				maxNumExPerPage);

		/* number bins of histogram */
		MWNumericArray numBinsScalar = new MWNumericArray((double) numBins);

		GenPatternQueryImgs queryImgGenertor = null;

		try {
			queryImgGenertor = new GenPatternQueryImgs();

			/* call the function */
			Object[] result = queryImgGenertor.gen_query_imgs(1,
					patternIdScalar, patternArray, matchedExamples,
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
				byte[] figureContent = FileUtils.readFileToByteArray(new File(
						figurePathStr));

				String figureName = null;
				int ind = figurePathStr.lastIndexOf(File.separator);

				if (ind == -1) {
					/* figure is written to current working directory */
					figureName = figurePathStr;
				} else {
					figureName = figurePathStr.substring(ind + 1);
				}

				figures.add(new PatternQueryFigure(figureName, figureContent));
			}

			FigureSet figureSet = new FigureSet();
			figureSet.setFigures(figures);

			VisualizationUtil.saveFigureSet(figureSet, finalOutputDir);

		} finally {
			if (queryImgGenertor != null) {
				queryImgGenertor.dispose();
			}

			matchedExamples.dispose();
			MWNumericArray.disposeArray(patternIdScalar);
			MWNumericArray.disposeArray(patternArray);
			MWNumericArray.disposeArray(allProbVector);
			MWNumericArray.disposeArray(matchedProbVector);
			MWNumericArray.disposeArray(allEventTypesVector);
			MWNumericArray.disposeArray(maxNumExPerPageScalar);
			MWNumericArray.disposeArray(numBinsScalar);
		}

		System.out.println("Done");
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws MWException
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String dataFilePath = "data/example-data-4.txt";
		int numTotalEvents = 3;
		int numTotalEx = 1000;
		int maxNumExPerPage = 4;
		int numBins = 20;
		String figureOutputDir = "testoutput";
		String figureFormat = ".png";
		String finalOutputDir = "figures";

		try {
			testVisFunction(dataFilePath, numTotalEvents, numTotalEx,
					maxNumExPerPage, numBins, figureOutputDir, figureFormat,
					finalOutputDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MWException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
