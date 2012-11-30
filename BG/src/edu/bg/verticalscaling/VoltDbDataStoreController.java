package edu.bg.verticalscaling;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltdb.client.ClientFactory;

import com.yahoo.ycsb.ArgumentException;
import com.yahoo.ycsb.BGException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.WorkloadException;

import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;
import edu.bg.verticalscaling.exceptions.WorkloadParameterException;

public class VoltDbDataStoreController extends DataStoreControllerImpl {

	private Process process;
	private final BGVerticalScaleOptions bgOptions;

	public VoltDbDataStoreController(BGVerticalScaleOptions bgOptions) {
		this.bgOptions = bgOptions;
	}

	@Override
	public void startDataStoreWithConfiguration(int ram_, int cores_,
			int threads_) {
		ProcessBuilder processBuilder = new ProcessBuilder("java", "-Xmx"
				+ ram_ + "m", "-Djava.library.path="
				+ bgOptions.getBGParameter("datastore_library_path"),
				"-Dlog4j.configuration=file://"
						+ bgOptions.getBGParameter("datastore_config_path"),
				"org.voltdb.VoltDB", "create", "host localhost",
				"catalog bg.jar", "deployment", "deployment_" + cores_ + ".xml");

		Map<String, String> env = processBuilder.environment();
		env.put("CLASSPATH", bgOptions.getBGParameter("classpath"));
		processBuilder.directory(new File(bgOptions
				.getBGParameter("config_dir")));

		try {
			process = processBuilder.start();
			errorGobbler = this.new StreamGobbler(process.getErrorStream());
			outputGobbler = new StreamGobbler(process.getInputStream());
			errorGobbler.start();
			outputGobbler.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void waitForDataStoreToStart() {
		while (true) {
			try {
				Thread.currentThread().sleep(5000);
				org.voltdb.client.Client client = ClientFactory.createClient();
				client.createConnection("localhost");
				break;
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void stopDataStoreWithConfiguration() {
		process.destroy();
	}

	@Override
	public void benchmark(Client bgClient, BGConfiguration configuration,
			BGVerticalScalingDriver bgVerticalScalingDriver)
			throws BGVerticalScaleParameterException, ArgumentException,
			IOException, WorkloadParameterException, BGException,
			UnknownDBException, WorkloadException, DBException {
		if (bgOptions.actions.contains("-load")) {
			List<String> args = bgVerticalScalingDriver.buildArgs(
					configuration, "-load", bgOptions.loadWorkloads.get(0));
			bgClient.doMain(args.toArray(new String[args.size()]));
		}
		if (bgOptions.actions.contains("-t")) {
			for (String workload : bgOptions.actionWorkloads) {
				List<String> args = bgVerticalScalingDriver.buildArgs(
						configuration, "-t", workload);
				bgClient.doMain(args.toArray(new String[args.size()]));
			}
		}
	}
}
