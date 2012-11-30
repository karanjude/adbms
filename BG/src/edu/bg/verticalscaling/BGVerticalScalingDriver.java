package edu.bg.verticalscaling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.yahoo.ycsb.ArgumentException;
import com.yahoo.ycsb.BGException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.WorkloadException;

import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;
import edu.bg.verticalscaling.exceptions.WorkloadParameterException;
import edu.bg.verticalscaling.util.ConfigGenerator;
import edu.bg.verticalscaling.util.JavaOptionParser;

public class BGVerticalScalingDriver {
	private final BGVerticalScaleOptions options;

	public BGVerticalScalingDriver(BGVerticalScaleOptions options,
			List<String> varargs) {
		this.options = options;
	}

	public static void main(String[] args) {
		List<String> varargs = Arrays.asList(args);

		BGVerticalScaleOptions options = JavaOptionParser.buildOptionMap(args);
		try {
			options.validateOptions();
		} catch (BGVerticalScaleParameterException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		BGVerticalScalingDriver driver = new BGVerticalScalingDriver(options,
				varargs);
		driver.runVerticalScalingBenchmark();
	}

	private void runVerticalScalingBenchmark() {
		List<List<String>> sets = new ArrayList<List<String>>();
		sets.add(options.actionWorkloads);
		sets.add(options.ram);
		sets.add(options.cores);
		sets.add(options.threads);
		sets.add(options.workloadOperationCounts);
		sets.add(options.workloadUserCounts);
		sets.add(options.workloadUserFriendCounts);
		sets.add(options.workloaduserResourceCounts);

		ConfigGenerator configGenerator = new ConfigGenerator(sets);
		int count = 0;
		while (configGenerator.next()) {
			String configValue = configGenerator.value();
			if (configValue.length() > 0) {
				System.out.println("RUNNING BG WITH CONFIG");
				BGConfiguration configuration = makeConfiguration(configValue);
				System.out.println(configValue);
				// count++;
				runBGForConfiguration(configuration);
				System.out.println("BG CONFIG RUN DONE");
			}
		}
		// System.out.println(count);
	}

	private BGConfiguration makeConfiguration(String configValue) {
		String[] vals = configValue.split("\\s+");
		String workload = vals[0];
		String ram = vals[1];
		String cores = vals[2];
		String threads = vals[3];
		String operationCount = vals[4];
		String workloadUserCount = vals[5];
		String workloadFriendCount = vals[6];
		String workloadResourceCount = vals[7];

		BGConfiguration configuration = new BGConfiguration();
		configuration.actionWorkload = workload;
		configuration.loadWorkload = options.loadWorkloads.get(0);
		configuration.ram = Integer.parseInt(ram);
		configuration.cores = Integer.parseInt(cores);
		configuration.threads = Integer.parseInt(threads);
		configuration.operationCount = Integer.parseInt(operationCount);
		configuration.workloadUserCount = Integer.parseInt(workloadUserCount);
		configuration.workloadFriendCount = Integer
				.parseInt(workloadFriendCount);
		configuration.workloadResourceCount = Integer
				.parseInt(workloadResourceCount);
		return configuration;
	}

	private void runBGForConfiguration(BGConfiguration configuration) {
		Client bgClient = new Client();
		boolean exit = false;

		DataStoreController dataStoreController = getDataStoreController(options);

		try {
			dataStoreController.startDataStoreWithConfiguration(
					configuration.ram, configuration.cores,
					configuration.threads);
			dataStoreController.waitForDataStoreToStart();
			System.out.println("Data Store started");

			dataStoreController.benchmark(bgClient, configuration, this);
		} catch (ArgumentException e) {
			exit = true;
			e.printStackTrace();
		} catch (IOException e) {
			exit = true;
			e.printStackTrace();
		} catch (WorkloadParameterException e) {
			exit = true;
			e.printStackTrace();
		} catch (BGException e) {
			exit = true;
			e.printStackTrace();
		} catch (UnknownDBException e) {
			exit = true;
			e.printStackTrace();
		} catch (WorkloadException e) {
			exit = true;
			e.printStackTrace();
		} catch (DBException e) {
			exit = true;
			e.printStackTrace();
		} catch (BGVerticalScaleParameterException e) {
			exit = true;
			e.printStackTrace();
		} finally {
			dataStoreController.stopDataStoreWithConfiguration();
			if (exit)
				System.exit(-1);
		}
	}

	private DataStoreController getDataStoreController(
			BGVerticalScaleOptions bgOptions) {
		if (options.datastore.toLowerCase().contains("voltdb")) {
			return new VoltDbDataStoreController(bgOptions);
		} else if (options.datastore.toLowerCase().contains("mysql")) {
			return new MysqlDbDataStoreController(bgOptions);
		}

		return null;
	}

	private void addParameter(List<String> result, String key, String value) {
		result.add("-p");
		result.add(key + "=" + value);
	}

	private String getResultFileName(String workloadName,
			BGConfiguration configuration, String action) {
		workloadName = workloadName.replaceAll("/", "_");
		action = action.replace("-", "_");
		StringBuilder result = new StringBuilder();
		result.append(workloadName).append("_")
				.append(configuration.operationCount).append("_")
				.append(configuration.workloadUserCount).append("_")
				.append(configuration.workloadFriendCount).append("_")
				.append(configuration.workloadResourceCount).append(action)
				.append("_").append(configuration.ram).append("_")
				.append(configuration.cores).append("_")
				.append(configuration.threads).append(".txt");
		return result.toString();
	}

	public List<String> buildArgs(BGConfiguration configuration, String action,
			String workload) {
		List<String> result = new ArrayList<String>();
		result.add(action);
		result.addAll(options.getBGParams());
		result.add("-P");
		result.add(workload);
		addParameter(result, "threadcount",
				new Integer(configuration.threads).toString());
		addParameter(result, "exportfile", options.exportFileBases.get(0)
				+ getResultFileName(workload, configuration, action));
		return null;
	}
}
