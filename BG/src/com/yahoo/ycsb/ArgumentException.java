package com.yahoo.ycsb;

public class ArgumentException extends Exception {

	private final String[] args;

	public ArgumentException(String[] args) {
		this.args = args;
	}

}
