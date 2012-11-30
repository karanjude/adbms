package edu.bg.verticalscaling.util;

import java.util.ArrayList;
import java.util.List;

public class ConfigGenerator {

	private final List<List<String>> sets;
	private int left;
	private int right;
	private int leftValue;
	private int rightValue;
	private ArrayList<Integer> currentValue;

	public ConfigGenerator(List<List<String>> sets) {
		this.sets = sets;
		left = sets.size() - 1;
		right = sets.size() - 1;
		leftValue = 0;
		rightValue = 0;
		this.currentValue = new ArrayList<Integer>();
		for (List<String> set : sets) {
			this.currentValue.add(0);
		}
	}

	public boolean next() {
		for (int i = 0; i < sets.size(); i++) {
			if (i == 0 && currentValue.get(i) >= sets.get(i).size()) {
				return false;
			} else if (currentValue.get(i) < sets.get(i).size()) {
				return true;
			}
		}
		return false;
	}

	public String value() {
		rightValue = currentValue.get(right);
		String result = "";
		if (rightValue < sets.get(right).size()) {
			result = makeString();
			currentValue.set(right, rightValue + 1);
		} else {
			boolean carry = false;
			for (int j = right; j >= 1; j--) {
				if (currentValue.get(j) >= sets.get(j).size()) {
					currentValue.set(j, 0);
					carry = true;
				}
				if (carry) {
					currentValue.set(j - 1, currentValue.get(j - 1) + 1);
				}
				carry = false;
			}
		}
		return result;
	}

	private String makeString() {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < currentValue.size(); i++) {
			int index = currentValue.get(i);
			result.append(sets.get(i).get(index));
			result.append(" ");
		}
		return result.toString();
	}

}
