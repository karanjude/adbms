package edu.bg.verticalscaling.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.bg.verticalscaling.BGVerticalScaleOptions;
import junit.framework.TestCase;

public class ConfigGeneratorTest extends TestCase {
	public void testname() throws Exception {
		BGVerticalScaleOptions options = new BGVerticalScaleOptions(
				new Properties());
		options.actions.add("load");
		options.ram.add("256");
		options.ram.add("512");
		options.cores.add("1");
		options.cores.add("2");
		options.threads.add("100");
		options.threads.add("75");
		options.workloadOperationCounts.add("10000");
		options.workloadOperationCounts.add("15000");
		options.workloadOperationCounts.add("20000");
		options.workloadOperationCounts.add("50000");
		options.workloadOperationCounts.add("100000");
		options.workloadOperationCounts.add("250000");
		options.actionWorkloads.add("workload1");
		options.actionWorkloads.add("workload2");
		options.actionWorkloads.add("workload3");
		options.actionWorkloads.add("workload4");

		List<List<String>> sets = new ArrayList<List<String>>();
		sets.add(options.actionWorkloads);
		sets.add(options.ram);
		sets.add(options.cores);
		sets.add(options.threads);
		sets.add(options.workloadOperationCounts);

		ConfigGenerator configGenerator = new ConfigGenerator(sets);

		while (configGenerator.next()) {
			String result = configGenerator.value();
			if (result.length() > 0)
				System.out.println(result);
		}

	}
}
