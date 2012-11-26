package com.yahoo.ycsb;

public class BGException extends Exception {

	private final Exception e;

	public BGException(Exception e) {
		this.e = e;
	}

}
