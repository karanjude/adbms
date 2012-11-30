package edu.bg.verticalscaling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import edu.bg.verticalscaling.exceptions.BGVerticalScaleParameterException;

public class BGVerticalScaleOptions {

	private static final String vert_workload_user_friend_count = "vert_workload_user_friend_count";
	private static final String vert_workload_user_resource_count = "vert_workload_user_resource_count";
	private static final String vert_mode = "vert_mode";
	private static final String vert_threads = "vert_threads";
	private static final String vert_export_file_base = "vert_export_file_base";
	public static final String bg_param_db = "bg_param_db";
	public static final String bg_param_db_user = "bg_param_db_user";
	public static final String bg_param_db_passwd = "bg_param_db_passwd";
	public static final String bg_param_db_url = "bg_param_db_url";
	public static final String bg_param_db_driver = "bg_param_db_driver";
	private static final String vert_ram = "vert_ram";
	private static final String vert_cores = "vert_cores";
	private static final String vert_load_workloads = "vert_load_workloads";
	private static final String vert_workload_dir = "vert_workload_dir";
	private static final String vert_action_workloads = "vert_action_workloads";
	private static final String vert_workload_operation_count = "vert_workload_operation_count";
	private static final String vert_workload_user_count = "vert_workload_user_count";
	public static final String config_dir = "config_dir";
	public static final String datastore_service = "datastore_service";

	private final Properties properties;

	public List<String> actions = new ArrayList<String>();
	public List<String> threads = new ArrayList<String>();;
	public List<String> ram = new ArrayList<String>();;
	public List<String> cores = new ArrayList<String>();;
	public List<String> loadWorkloads = new ArrayList<String>();;
	public List<String> workloadDirectory = new ArrayList<String>();;
	public List<String> actionWorkloads = new ArrayList<String>();;
	public List<String> workloadOperationCounts = new ArrayList<String>();;
	public List<String> workloadUserCounts = new ArrayList<String>();;
	public List<String> workloadUserFriendCounts = new ArrayList<String>();;
	public List<String> workloaduserResourceCounts = new ArrayList<String>();;
	public List<String> exportFileBases = new ArrayList<String>();

	private StringBuilder bgCommandLine = new StringBuilder();
	public String datastore;
	private List<String> bgParams = new ArrayList<String>();

	public BGVerticalScaleOptions(Properties properties) {
		this.properties = properties;
	}

	public void printOptions() {
		this.properties.list(System.out);
	}

	public void validateOptions() throws BGVerticalScaleParameterException {
		validateParamter(vert_threads, this.threads);
		validateParamter(vert_ram, this.ram);
		validateParamter(vert_cores, this.cores);
		validateParamter(vert_load_workloads, this.loadWorkloads);
		validateParamter(vert_workload_dir, this.workloadDirectory);
		validateParamter(vert_action_workloads, this.actionWorkloads);
		validateParamter(vert_workload_operation_count,
				this.workloadOperationCounts);
		validateParamter(vert_workload_user_count, this.workloadUserCounts);
		validateParamter(vert_workload_user_friend_count,
				this.workloadUserFriendCounts);
		validateParamter(vert_workload_user_resource_count,
				this.workloaduserResourceCounts);
		validateParamter(vert_mode, this.actions);
		validateParamter(vert_export_file_base, this.exportFileBases);
		buildBGParam(bg_param_db, bgCommandLine, "-db", "");
		this.datastore = properties.getProperty(bg_param_db);
		buildBGParam(bg_param_db_user, bgCommandLine, "-p", "db.user");
		buildBGParam(bg_param_db_passwd, bgCommandLine, "-p", "db.passwd");
		buildBGParam(bg_param_db_url, bgCommandLine, "-p", "db.url");
		buildBGParam(bg_param_db_driver, bgCommandLine, "-p", "db.driver");
	}

	private void buildBGParam(String key, StringBuilder toAdd,
			String paramToken, String bgkey) {
		if (properties.contains(key)) {
			bgParams.add(paramToken);
			if (bgkey.length() == 0) {
				bgParams.add(properties.getProperty(key));
			} else {
				bgParams.add(bgkey + "=" + properties.getProperty(key, ""));
			}
		}
	}

	private void validateParamter(String parameterKey, List<String> toSet)
			throws BGVerticalScaleParameterException {
		if (!properties.containsKey(parameterKey)) {
			throw new BGVerticalScaleParameterException("no " + parameterKey
					+ " specified");
		}
		String v = properties.getProperty(parameterKey, "");
		String[] values = v.split("\\s+");
		toSet.addAll(Arrays.asList(values));
	}

	public List<String> getBGParams() {
		return bgParams;
	}

	public boolean containsBGParameter(String key) {
		return properties.containsKey(key);
	}

	public String getBGParameter(String key) {
		return properties.getProperty(key);
	}

}
