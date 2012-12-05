package edu.bg.verticalscaling.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.bg.verticalscaling.BGVerticalScaleOptions;
import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;

public class JavaOptionParser {

	public static void main(String[] args) {
		BGVerticalScaleOptions options = buildOptionMap(args);
		try {
			options.validateOptions();
			options.printOptions();
			ConfigGenerator configGenerator = new ConfigGenerator(null);
			while(configGenerator.next()){
				System.out.println(configGenerator.value());
			}
		} catch (BGVerticalScaleParameterException e) {
			e.printStackTrace();
		}
	}

	public static BGVerticalScaleOptions buildOptionMap(String[] args) {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(args[1]));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return new BGVerticalScaleOptions(properties);
	}

}
