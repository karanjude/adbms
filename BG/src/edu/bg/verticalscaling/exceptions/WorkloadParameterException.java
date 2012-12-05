package edu.bg.verticalscaling.exceptions;

import java.util.Properties;

public class WorkloadParameterException extends Exception {

	private final Properties props;

	public WorkloadParameterException(Properties props) {
		this.props = props;
	}

}
