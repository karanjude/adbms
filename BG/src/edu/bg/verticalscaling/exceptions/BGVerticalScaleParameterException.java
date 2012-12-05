package edu.bg.verticalscaling.exceptions;

public class BGVerticalScaleParameterException extends Exception {

	private final String message;

	public BGVerticalScaleParameterException(String message) {
		this.message = message;
	}

}
