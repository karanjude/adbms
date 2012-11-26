package com.yahoo.ycsb;

public class ProbabilityException extends Exception {

	private final String message;

	public ProbabilityException(String message) {
		this.message = message;
	}

}
