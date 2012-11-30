package edu.bg.verticalscaling;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.maven.artifact.ant.shaded.FileUtils;

import com.yahoo.ycsb.ArgumentException;
import com.yahoo.ycsb.BGException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.WorkloadException;

import edu.bg.verticalscaling.DataStoreControllerImpl.StreamGobbler;
import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;
import edu.bg.verticalscaling.exceptions.WorkloadParameterException;

public class MysqlDbDataStoreController extends DataStoreControllerImpl {

	private Process process;
	private Connection conn;
	private final BGVerticalScaleOptions bgOptions;

	public MysqlDbDataStoreController(BGVerticalScaleOptions bgOptions) {
		this.bgOptions = bgOptions;
	}

	@Override
	public void startDataStoreWithConfiguration(int ram_, int cores_,
			int threads_) {
		String src = "my-" + ram_ + "-" + cores_ + ".cnf";
		String dest = "/etc/my.cnf";
		ProcessBuilder processBuilder = new ProcessBuilder("cp", src, dest);
		// processBuilder.directory(new File(
		// "/Users/jude/dev/dbms/dbmsprj/adbms/BG/db/mysql/scripts"));
		processBuilder.directory(new File(bgOptions
				.getBGParameter(BGVerticalScaleOptions.config_dir)));

		try {
			process = processBuilder.start();
			errorGobbler = this.new StreamGobbler(process.getErrorStream());
			outputGobbler = new StreamGobbler(process.getInputStream());

			errorGobbler.start();
			outputGobbler.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// processBuilder = new ProcessBuilder(
		// "/opt/local/etc/LaunchDaemons/org.macports.mysql5/mysql5.wrapper",
		// "restart");
		
		processBuilder = new ProcessBuilder(
				bgOptions.getBGParameter(BGVerticalScaleOptions.datastore_service),
				"restart");

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
	public void waitForDataStoreToStart()
			throws BGVerticalScaleParameterException {
		String urls = getKey(BGVerticalScaleOptions.bg_param_db_url);
		String user = getKey(BGVerticalScaleOptions.bg_param_db_user);
		String passwd = getKey(BGVerticalScaleOptions.bg_param_db_passwd);
		String driver = getKey(BGVerticalScaleOptions.bg_param_db_driver);

		while (true) {
			if (conn != null)
				break;
			try {
				Thread.currentThread().sleep(5000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			try {
				Class.forName(driver);
				for (String url : urls.split(",")) {
					conn = DriverManager.getConnection(url, user, passwd);
				}
			} catch (ClassNotFoundException e) {
				System.out.println("Error in initializing the JDBS driver: "
						+ e);
			} catch (SQLException e) {
				System.out.println("Error in database operation: " + e);
			} catch (NumberFormatException e) {
				System.out.println("Invalid value for fieldcount property. "
						+ e);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	private String getKey(String key) throws BGVerticalScaleParameterException {
		if (!bgOptions.containsBGParameter(key))
			throw new BGVerticalScaleParameterException(key + " not found");
		return bgOptions.getBGParameter(key);
	}

	@Override
	public void stopDataStoreWithConfiguration() {

	}

	@Override
	public void benchmark(Client bgClient, BGConfiguration configuration,
			BGVerticalScalingDriver bgVerticalScalingDriver)
			throws BGVerticalScaleParameterException, ArgumentException,
			IOException, WorkloadParameterException, BGException,
			UnknownDBException, WorkloadException, DBException {

		if (bgOptions.actions.contains("-load")) {
			List<String> args = new ArrayList<String>();
			args.add("-schema");
			args.addAll(bgOptions.getBGParams());
			bgClient.doMain(args.toArray(new String[args.size()]));

			args = bgVerticalScalingDriver.buildArgs(configuration, "-load",
					bgOptions.loadWorkloads.get(0));
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
