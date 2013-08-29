package edu.indiana.d2i.seqmining.vis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import edu.indiana.d2i.seqmining.Constants;
import edu.indiana.d2i.seqmining.ds.FigureSet;
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.ds.VisQueryCriteria;
import edu.indiana.d2i.seqmining.util.AprioriUtils;
import edu.indiana.d2i.seqmining.util.VisualizationUtil;

public class VisualizationDriver {

	private static String usage = "Usage: java edu.indiana.d2i.seqmining.vis.VisualizationDriver"
			+ " <binary file of frequent patterns> <num map tasks> <num reduce tasks> <partition file> <property file>";

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String partitionFile, int numMapTasks,
			int numReduceTasks, String patternBinaryFilePath, Properties prop)
			throws TwisterException, IOException, SerializationException {

		// job configurations
		JobConf jobConf = new JobConf("visualization-"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(VisualizationMapTask.class);
		jobConf.setReducerClass(VisualizationReduceTask.class);
		jobConf.setCombinerClass(VisualizationCombiner.class);
		/**
		 * set the reduce selector, namely partitioner in Hadoop's terminology
		 */
		jobConf.setReducerSelectorClass(VisualizationReducerSelector.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		// jobConf.setFaultTolerance();

		// set visualization related properties
		jobConf.addProperty(
				Constants.VIS_MAX_NUM_MATCHED_EXAMPLES_DISPLAY,
				prop.getProperty(Constants.VIS_MAX_NUM_MATCHED_EXAMPLES_DISPLAY));

		jobConf.addProperty(Constants.VIS_FIGURE_REDUCE_OUTPUT_DIR,
				prop.getProperty(Constants.VIS_FIGURE_REDUCE_OUTPUT_DIR));

		jobConf.addProperty(Constants.VIS_FIGURE_FORMAT,
				prop.getProperty(Constants.VIS_FIGURE_FORMAT));

		jobConf.addProperty(Constants.VIS_FIGURE_MAX_NUM_EX_PER_PAGE,
				prop.getProperty(Constants.VIS_FIGURE_MAX_NUM_EX_PER_PAGE));

		jobConf.addProperty(Constants.VIS_FIGURE_HISTO_NUM_BIN,
				prop.getProperty(Constants.VIS_FIGURE_HISTO_NUM_BIN));

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);

		// load patterns from binary file
		PatternSet patterns = AprioriUtils
				.loadPatternsFromBinaryFile(patternBinaryFilePath);

		// filter pattern by query criteria
		PatternSet passedPatterns = VisualizationUtil.filterPatternByCriteria(
				patterns, VisQueryCriteria.valueOf(prop
						.getProperty(Constants.VIS_QUERY_CRITERIA)), prop);

		TwisterMonitor monitor = null;

		/* visualization is non-iterative */
		monitor = driver.runMapReduceBCast(passedPatterns);
		monitor.monitorTillCompletion();

		FigureSet figureSet = ((VisualizationCombiner) driver
				.getCurrentCombiner()).getResults();

		VisualizationUtil.saveFigureSet(figureSet,
				prop.getProperty(Constants.VIS_FIGURE_CLIENT_OUTPUT_DIR));

		// close driver
		driver.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		// TODO Auto-generated method stub

		if (args.length != 5) {
			System.err.println(usage);
			System.exit(1);
		}

		String patternBinaryFilePath = args[0];
		int numMapTasks = Integer.parseInt(args[1]);
		int numReduceTasks = Integer.parseInt(args[2]);
		String partitionFile = args[3];

		Properties prop = new Properties();
		prop.load(new FileInputStream(args[4]));

		VisualizationDriver driver = null;

		try {
			driver = new VisualizationDriver();
			double beginTime = System.currentTimeMillis();
			driver.driveMapReduce(partitionFile, numMapTasks, numReduceTasks,
					patternBinaryFilePath, prop);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("Visualization procedure took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
