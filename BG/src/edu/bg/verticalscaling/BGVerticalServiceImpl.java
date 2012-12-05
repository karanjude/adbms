package edu.bg.verticalscaling;

import org.apache.thrift.TException;

import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;

public class BGVerticalServiceImpl implements BGService.Iface {

	private String datastore;
	private final BGVerticalScaleOptions options;
	private DataStoreController dataStoreController;

	public BGVerticalServiceImpl(BGVerticalScaleOptions options) {
		this.options = options;
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

	@Override
	public void stop(String datastore) throws TException {
		System.out.println("Stopping..." + datastore);
		dataStoreController.stopDataStoreWithConfiguration();
	}

	@Override
	public void exit() throws TException {
		System.exit(0);
	}

	@Override
	public void start(String datastore, int ram, int cores, int threads)
			throws TException {
		this.datastore = datastore;
		dataStoreController = getDataStoreController(options);

		dataStoreController
				.startDataStoreWithConfiguration(ram, cores, threads);
		try {
			dataStoreController.waitForDataStoreToStart();
			System.out.println("Starting..." + datastore);
			System.out.println("Data Store started");
		} catch (BGVerticalScaleParameterException e) {
			e.printStackTrace();
		}

	}

}
