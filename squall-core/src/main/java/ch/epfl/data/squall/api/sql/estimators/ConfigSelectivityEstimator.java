package ch.epfl.data.squall.api.sql.estimators;

import java.util.Map;

import ch.epfl.data.squall.utilities.SystemParameters;

public class ConfigSelectivityEstimator {

	private final Map _map;

	public ConfigSelectivityEstimator(Map map) {
		_map = map;
	}

	/*
	 * read selectivity from the config file
	 */
	public double estimate(String compName) {
		final String selStr = compName + "_SEL";
		return SystemParameters.getDouble(_map, selStr);
	}

}
