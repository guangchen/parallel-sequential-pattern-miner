package edu.indiana.d2i.seqmining.adjust;

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
import edu.indiana.d2i.seqmining.ds.PatternSet;
import edu.indiana.d2i.seqmining.util.AprioriUtils;
import edu.indiana.d2i.seqmining.util.ProbAdjustUtil;

public class ProbAdjustDriver {

	private static String usage = "Usage: java edu.indiana.d2i.seqmining.adjust.ProbAdjustDriver"
			+ " <binary file of frequent patterns> <num map tasks> <partition file> <property file>";

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String partitionFile, int numMapTasks,
			String patternBinaryFilePath, Properties prop)
			throws TwisterException, IOException, SerializationException {

		int numReducers = 1; // we need only one reducer

		// job configurations
		JobConf jobConf = new JobConf("prob-adjust-"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(ProbAdjustMapTask.class);
		jobConf.setReducerClass(ProbAdjustReduceTask.class);
		jobConf.setCombinerClass(ProbAdjustCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		// jobConf.setFaultTolerance();

		// set algorithm related properties
		jobConf.addProperty(Constants.EVENT_MATCHER_OVERLAP_LENGTH_RATIO,
				prop.getProperty(Constants.EVENT_MATCHER_OVERLAP_LENGTH_RATIO));

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);

		// load patterns from binary file
		PatternSet patterns = AprioriUtils
				.loadPatternsFromBinaryFile(patternBinaryFilePath);

		TwisterMonitor monitor = null;

		/* Probabilistic adjustment procedure is non-iterative */
		monitor = driver.runMapReduceBCast(patterns);
		monitor.monitorTillCompletion();

		PatternSet adjustedPatterns = ((ProbAdjustCombiner) driver
				.getCurrentCombiner()).getResults();

		// save patterns to text file
		AprioriUtils.savePatternsToTextFile(adjustedPatterns,
				prop.getProperty(Constants.ADJUSTED_PATTERN_TEXT_OUTFILE_PATH));

		// add mapping table
		ProbAdjustUtil.generateFullPatternSet(patterns, adjustedPatterns);

		// save adjusted patterns to binary file
		AprioriUtils.savePatternsToBinaryFile(adjustedPatterns, prop
				.getProperty(Constants.ADJUSTED_PATTERN_BINARY_OUTFILE_PATH));

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

		if (args.length != 4) {
			System.err.println(usage);
			System.exit(1);
		}

		String patternBinaryFilePath = args[0];
		int numMapTasks = Integer.parseInt(args[1]);
		String partitionFile = args[2];

		Properties prop = new Properties();
		prop.load(new FileInputStream(args[3]));

		ProbAdjustDriver driver = null;

		try {
			driver = new ProbAdjustDriver();
			double beginTime = System.currentTimeMillis();
			driver.driveMapReduce(partitionFile, numMapTasks,
					patternBinaryFilePath, prop);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("Probabilistic adjustment procedure took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
