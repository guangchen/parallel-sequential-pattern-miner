package edu.indiana.d2i.seqmining;

public class Constants {

	// properties related with dataset
	public static String DATASET_NUM_EXAMPLES = "dataset.num.examples";

	// properties related with clustering
	// maximum # of iteration in clustering
	public static String CLUSTERING_MAX_NUM_ITER = "clustering.max.num.iter";

	// threshold of convergence
	public static String CLUSTERING_CONVERGE_THRESHOLD = "clustering.converge.threshold";

	// path of output file for converged centroids
	public static String CLUSTERING_OUTFILE_PATH = "clustering.outfile.path";

	// properties related with apriori
	public static String APRIORI_MAX_PATTERN_LEN = "apriori.max.pattern.length";

	// minimum support probability for a pattern to be considered frequent
	public static String APRIORI_MIN_SUP_PROB = "apriori.min.sup.prob";

	// whether use penalty in event match
	public static String APRIORI_EVENT_MATCH_USE_PENALTY = "apriori.event.match.use.penalty";

	// penalty threshold
	public static String APRIORI_EVENT_MATCH_PENALTY_THRESHOLD = "apriori.event.match.penalty.threshold";

	// penalty factor
	public static String APRIORI_EVENT_MATCH_PENALTY_FACTOR = "apriori.event.match.penalty.factor";
	
	// similarity threshold  given an example and a pattern
	public static String APRIORI_PATTERN_MATCH_THRESHOLD = "apriori.pattern.match.threshold";
	
	// threshold that used to remove 'prefix' and 'included' patterns
	public static String APRIORI_PATTERN_REMOVAL_THRESHOLD = "apriori.pattern.removal.threshold";

	// path of text output file for discovered frequent patterns
	public static String FREQUENT_PATTERN_TEXT_OUTFILE_PATH = "frequent.pattern.text.file.path";

	// path of binary output file for discovered frequent patterns
	public static String FREQUENT_PATTERN_BINARY_OUTFILE_PATH = "frequent.pattern.binary.file.path";

	// properties related with probabilistic adjustment
	public static String ADJUSTED_PATTERN_TEXT_OUTFILE_PATH = "adjusted.pattern.text.file.path";

	public static String ADJUSTED_PATTERN_BINARY_OUTFILE_PATH = "adjusted.pattern.binary.file.path";

	public static String EVENT_MATCHER_OVERLAP_LENGTH_RATIO = "event.matcher.overlap.length.raio";

	// properties related with visualization

	// maximum number of matched examples to display for a discovered pattern
	public static String VIS_QUERY_CRITERIA = "vis.query.criteria";
	// minimum number of matched examples, an absolute measure
	public static String VIS_MIN_NUM_MATCHED_EXAMPLES = "vis.min.num.matched.examples";
	// minimum ratio of matched examples, which is (# of matched examples) / (#
	// of total examples), a relative measure
	public static String VIS_MIN_RATIO_MATCHED_EXAMPLES = "vis.min.ratio.matched.examples";
	// minimum averaged matching ratio, calculated as totolProb / (# of matched
	// examples)
	public static String VIS_MIN_AVERAGED_MATCHING_PROB = "vis.min.averaged.matching.prob";

	public static String VIS_MAX_NUM_MATCHED_EXAMPLES_DISPLAY = "vis.max.num.matched.examples.display";

	public static String VIS_FIGURE_REDUCE_OUTPUT_DIR = "vis.figure.reduce.output.dir";

	public static String VIS_FIGURE_FORMAT = "vis.figure.format";

	public static String VIS_FIGURE_MAX_NUM_EX_PER_PAGE = "vis.figure.max.num.ex.per.page";

	public static String VIS_FIGURE_HISTO_NUM_BIN = "vis.figure.histo.num.bin";

	public static String VIS_FIGURE_CLIENT_OUTPUT_DIR = "vis.figure.client.output.dir";
}
