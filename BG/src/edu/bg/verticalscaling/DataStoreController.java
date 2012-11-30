package edu.bg.verticalscaling;

import java.io.IOException;

import com.yahoo.ycsb.ArgumentException;
import com.yahoo.ycsb.BGException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.WorkloadException;

import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;
import edu.bg.verticalscaling.exceptions.WorkloadParameterException;

public interface DataStoreController {

	void startDataStoreWithConfiguration(int ram_, int cores_, int threads_);

	void waitForDataStoreToStart() throws BGVerticalScaleParameterException;

	void stopDataStoreWithConfiguration();

	
	void benchmark(Client bgClient, BGConfiguration configuration,
			BGVerticalScalingDriver bgVerticalScalingDriver) throws BGVerticalScaleParameterException, ArgumentException, IOException, WorkloadParameterException, BGException, UnknownDBException, WorkloadException, DBException;

}
