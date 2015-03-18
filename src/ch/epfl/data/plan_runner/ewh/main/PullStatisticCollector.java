package ch.epfl.data.plan_runner.ewh.main;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.plan_runner.ewh.visualize.UJMPVisualizer;
import ch.epfl.data.plan_runner.ewh.visualize.VisualizerInterface;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.utilities.CustomReader;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SerializableFileInputStream;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class PullStatisticCollector {
	private static class Hyracks implements QueryPlan {
		private IntegerConversion _ic = new IntegerConversion();

		private Map _map;
		private String _dataPath, _extension;

		public Hyracks(Map map) {
			_map = map;
			_dataPath = SystemParameters.getString(_map, "DIP_DATA_PATH") + "/";
			_extension = SystemParameters.getString(_map, "DIP_EXTENSION");
		}

		@Override
		public JoinMatrix generateMatrix() {
			CustomReader firstReader = open(_dataPath + "customer" + _extension);
			ProjectOperator projectionFirst = new ProjectOperator(
					new int[] { 0 });
			ChainOperator firstOps = new ChainOperator(projectionFirst);
			List<Integer> firstJoinKeys = readAllTupleJoinKeys(firstReader,
					firstOps, _ic, _map);
			Collections.sort(firstJoinKeys);

			CustomReader secondReader = open(_dataPath + "orders" + _extension);
			ProjectOperator projectionSecond = new ProjectOperator(
					new int[] { 1 });
			ChainOperator secondOps = new ChainOperator(projectionSecond);
			List<Integer> secondJoinKeys = readAllTupleJoinKeys(secondReader,
					secondOps, _ic, _map);
			Collections.sort(secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.EQUAL_OP);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					firstJoinKeys.size(), secondJoinKeys.size(), _map);
			fillMatrix(joinMatrix, firstJoinKeys, secondJoinKeys, comparison);
			return joinMatrix;
		}
	}

	private static interface QueryPlan {
		public JoinMatrix generateMatrix();
	}

	private static void close(CustomReader reader) {
		try {
			reader.close();
		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
		}
	}

	private static <T extends Comparable<T>> void fillMatrix(
			JoinMatrix joinMatrix, List<T> firstKeys, List<T> secondKeys,
			ComparisonPredicate comparison) {
		int xSize = firstKeys.size();
		int ySize = secondKeys.size();
		for (int i = 0; i < xSize; i++) {
			for (int j = 0; j < ySize; j++) {
				if (comparison.test(firstKeys.get(i), secondKeys.get(j))) {
					joinMatrix.setElement(1, i, j);
				}
			}
		}
	}

	public static void main(String[] args) {
		new PullStatisticCollector(args);
	}

	private static CustomReader open(String inputPath) {
		CustomReader reader = null;
		try {
			reader = new SerializableFileInputStream(new File(inputPath));

		} catch (final Exception e) {
			final String error = MyUtilities.getStackTrace(e);
			LOG.info(error);
			throw new RuntimeException("Filename not found:" + error);
		}
		return reader;
	}

	private static void printWallClockTime() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		LOG.info(dateFormat.format(cal.getTime()));
	}

	private static <T extends Comparable<T>> List<T> readAllTupleJoinKeys(
			CustomReader reader, ChainOperator operators,
			TypeConversion<T> conv, Map map) {
		List<T> tupleKeys = new ArrayList<T>();
		T key = null;
		while ((key = readTupleJoinKey(reader, operators, conv, map)) != null) {
			tupleKeys.add(key);
		}
		return tupleKeys;
	}

	private static String readLine(CustomReader reader) {
		String text = null;
		try {
			text = reader.readLine();
		} catch (final IOException e) {
			final String errMessage = MyUtilities.getStackTrace(e);
			LOG.info(errMessage);
		}
		return text;
	}

	private static List<String> readTuple(CustomReader reader,
			ChainOperator operators, Map map) {
		String line = null;
		List<String> tuple = null;
		while (tuple == null && (line = readLine(reader)) != null) {
			tuple = MyUtilities.fileLineToTuple(line, map);
			tuple = operators.process(tuple,-1);
		}
		return tuple;
	}

	private static <T extends Comparable<T>> T readTupleJoinKey(
			CustomReader reader, ChainOperator operators,
			TypeConversion<T> conv, Map map) {
		List<String> tuple = readTuple(reader, operators, map);
		return tuple != null ? conv.fromString(tuple.get(0)) : null;
	}

	private static Logger LOG = Logger.getLogger(PullStatisticCollector.class);

	public PullStatisticCollector(String args[]) {
		String configDirectory = args[0];
		List<String> configPaths = MyUtilities
				.listFilesForPath(configDirectory);
		LOG.info("There are " + configPaths.size()
				+ " config files to process.");

		int serial = 0;
		for (String confPath : configPaths) {
			LOG.info("Processing config file number " + serial + ".");
			Config conf = SystemParameters.fileToStormConfig(confPath);
			try {
				processConfig(conf);
			} catch (Exception exc) {
				LOG.info("EXCEPTION" + MyUtilities.getStackTrace(exc));
			}
			serial++;
		}
	}

	// change from here down
	private QueryPlan chooseQueryPlan(Map map) {
		String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
		QueryPlan generator = null;
		if (queryName.equalsIgnoreCase("hyracks")) {
			generator = new Hyracks(map);
		} else {
			throw new RuntimeException("Unsupported query plan " + queryName
					+ "!");
		}
		return generator;
	}

	private void processConfig(Map map) {
		printWallClockTime();
		long startTime = System.currentTimeMillis();

		// generate matrix
		QueryPlan generator = chooseQueryPlan(map);
		JoinMatrix joinMatrix = generator.generateMatrix();
		String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");

		// optionally write matrix to a file
		joinMatrix.writeMatrixToFile();

		// visualize without regions
		String label = queryName;
		VisualizerInterface visualizer = new UJMPVisualizer(label);
		joinMatrix.visualize(visualizer);

		// visualize with regions
		Region region = new Region(0, 0, 300, 3000);
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		joinMatrix.setRegions(regions);
		label += " with regions";
		visualizer = new UJMPVisualizer(label);
		joinMatrix.visualize(visualizer);

		long endTime = System.currentTimeMillis();
		double elapsed = (endTime - startTime) / 1000.0;
		LOG.info("Elapsed time with visualization and writing to file for "
				+ queryName + " of size [" + joinMatrix.getXSize() + ", "
				+ joinMatrix.getYSize() + "]" + " is " + elapsed + " seconds.");
	}
}