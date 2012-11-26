package com.yahoo.ycsb;

public class FragmentSizeTooSmallException extends Exception {

	private final String message;

	public FragmentSizeTooSmallException(String message) {
		this.message = message;
	}

}
