package edu.indiana.d2i.seqmining.util;

import java.util.List;

import edu.indiana.d2i.seqmining.ds.Event;
import edu.indiana.d2i.seqmining.ds.PatternInAdjustSet;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust;
import edu.indiana.d2i.seqmining.ds.SeqPatternInAdjust.EventInAdjust;
import edu.indiana.d2i.seqmining.ds.SequentialPattern;

public class ProbAdjustUtil {

	public static PatternSet patternInAdjust2Pattern(
			PatternInAdjustSet patternInAdjustSet) {
		PatternSet patterns = new PatternSet();

		for (SeqPatternInAdjust patternInAdjust : patternInAdjustSet
				.getPatterns()) {
			SequentialPattern pattern = new SequentialPattern();

			for (EventInAdjust ev : patternInAdjust.getPatternInAdjust()) {
				float startTime = ev.getSumedStartTime();
				float endTime = ev.getSumedEndTime();

				if (ev.getNumEvents() > 0) {
					startTime /= (float) ev.getNumEvents();
					endTime /= (float) ev.getNumEvents();
				}

				pattern.addEvent(new Event(ev.getEventType(), startTime,
						endTime - startTime));

			}

			patterns.addPattern(pattern);
		}

		return patterns;
	}

	/**
	 * generate a full pattern set, basically it adds mapping tables from
	 * original patterns to adjusted patterns
	 * 
	 * @param original
	 *            original unadjusted patterns with mapping table
	 * @param adjusted
	 *            adjusted patterns without mapping table
	 * @return
	 */
	public static void generateFullPatternSet(PatternSet original,
			PatternSet adjusted) {

		List<SequentialPattern> originalPatterns = original.getPatterns();
		List<SequentialPattern> adjustedPatterns = adjusted.getPatterns();

		assert originalPatterns.size() == adjustedPatterns.size();

		for (int i = 0; i < adjustedPatterns.size(); i++) {
			adjustedPatterns.get(i).setMatchExScore(
					originalPatterns.get(i).getMatchExScore());
			adjustedPatterns.get(i).setTotalScore(
					originalPatterns.get(i).getTotalScore());
		}

	}

}
