package edu.indiana.d2i.seqmining.util;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.FigureSet;
import edu.indiana.d2i.seqmining.ds.PatternQueryFigure;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;
import edu.indiana.d2i.seqmining.ds.VisQueryCriteria;

public class VisualizationUtil {

	public static PatternSet filterPatternByCriteria(PatternSet pset,
			VisQueryCriteria criteria, Properties prop) {

		switch (criteria) {

		case MIN_NUM_MATCHED_EXAMPLES:
			return filterPatternByMinExNum(pset, Integer.parseInt(prop
					.getProperty(Constants.VIS_MIN_NUM_MATCHED_EXAMPLES)));

		case MIN_RATIO_MATCHED_EXAMPLES:
			return filterPatternByMinExRatio(pset, Float.parseFloat(prop
					.getProperty(Constants.VIS_MIN_RATIO_MATCHED_EXAMPLES)),
					Long.parseLong(prop
							.getProperty(Constants.DATASET_NUM_EXAMPLES)));

		case MIN_AVERAGED_MATCHING_PROB:
			return filterPatternByMinAvgProb(pset, Float.parseFloat(prop
					.getProperty(Constants.VIS_MIN_AVERAGED_MATCHING_PROB)),
					Long.parseLong(prop
							.getProperty(Constants.DATASET_NUM_EXAMPLES)));

		default:
			System.err.println("Unsupported query criteria : " + criteria);
		}

		// should not reach here
		return new PatternSet();
	}

	private static PatternSet filterPatternByMinExNum(PatternSet pset,
			int minExampleNum) {
		PatternSet patterns = new PatternSet();

		for (SequentialPattern p : pset.getPatterns()) {
			if (p.getMatchedExampleId().size() > minExampleNum) {
				patterns.addPattern(p);
			}
		}
		return patterns;
	}

	private static PatternSet filterPatternByMinExRatio(PatternSet pset,
			float minRatio, long totalExNum) {
		PatternSet patterns = new PatternSet();

		for (SequentialPattern p : pset.getPatterns()) {
			float ratio = (float) p.getMatchedExampleId().size()
					/ (float) totalExNum;

			if (ratio > minRatio) {
				patterns.addPattern(p);
			}
		}
		return patterns;
	}

	private static PatternSet filterPatternByMinAvgProb(PatternSet pset,
			float minAvgProb, long totalExNum) {
		PatternSet patterns = new PatternSet();

		for (SequentialPattern p : pset.getPatterns()) {
			float avgProb = (float) p.getTotalScore() / (float) totalExNum;

			if (avgProb > minAvgProb) {
				patterns.addPattern(p);
			}
		}
		return patterns;
	}

	public static void saveFigureSet(FigureSet figureSet, String outputDir)
			throws IOException {
		for (PatternQueryFigure figure : figureSet.getFigures()) {

			String figureName = figure.getFigureName();
			byte[] figureContent = figure.getContent();

			StringBuilder filePath = new StringBuilder();
			filePath.append(outputDir).append(File.separator)
					.append(figureName);

			FileUtils.writeByteArrayToFile(new File(filePath.toString()),
					figureContent);
		}
	}
}
