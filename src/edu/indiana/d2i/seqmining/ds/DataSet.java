package edu.indiana.d2i.seqmining.ds;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * class which represents a collection of examples
 * 
 * @author Guangchen
 * 
 */
public class DataSet {
	private List<Example> examples;

	public DataSet(String dsFilePath) throws IOException {
		examples = new ArrayList<Example>();
		loadDataset(dsFilePath);
	}

	/**
	 * The dataset plain text file has following format, <> indicating a line:
	 * ---- begin dataset file ---- <# of examples> <example_id interval>
	 * <event_type1 event_type2...> <start_time1 start_time2...> <duration1
	 * duration2...> <another example ...> ---- end dataset file ----
	 * 
	 * The very first line is the number of examples in the text file. Each
	 * example spans 4 lines, first line is example id and example interval,
	 * second line is event type, third and fourth lines are start time and
	 * event duration respectively.
	 * 
	 * @param dsFilePath
	 *            path to the dataset file
	 * @throws IOException
	 */
	private void loadDataset(String dsFilePath) throws IOException {

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(dsFilePath)));

			long numExamples = Long.parseLong(reader.readLine().trim());

			for (long i = 0; i < numExamples; i++) {
				/* split by whitespace characters */
				String[] tokens = reader.readLine().trim().split("\\s+");

				assert tokens.length == 2;

				// need deal with scientific notation
				// long exampleId = Long.parseLong(tokens[0]);
				long exampleId = Double.valueOf(tokens[0]).longValue();
				float interval = Float.parseFloat(tokens[1]);

				String[] eventTypes = reader.readLine().trim().split("\\s+");
				String[] startTimes = reader.readLine().trim().split("\\s+");
				String[] durations = reader.readLine().trim().split("\\s+");

				/* lengths should all be equal */
				assert (eventTypes.length == startTimes.length)
						&& (eventTypes.length == durations.length);

				Example ex = new Example(exampleId, interval);

				for (int j = 0; j < eventTypes.length; j++) {
					ex.addEvent(new Event(Integer.parseInt(eventTypes[j]),
							Float.parseFloat(startTimes[j]), Float
									.parseFloat(durations[j])));
				}

				examples.add(ex);
			}
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	public List<Example> getExamples() {
		return examples;
	}

	public Map<Long, Example> getExamplesById(Set<Long> ids) {
		Map<Long, Example> exs = new HashMap<Long, Example>();

		for (Example e : examples) {
			if (ids.contains(e.getId())) {
				exs.put(e.getId(), e);
			}
		}

		return exs;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (Example ex : examples) {
			sb.append(ex.toString());
		}

		return sb.toString();
	}
}
