/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.ewh.main;

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

import org.apache.storm.Config;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.squall.ewh.visualize.UJMPVisualizer;
import ch.epfl.data.squall.ewh.visualize.VisualizerInterface;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.CustomReader;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SerializableFileInputStream;
import ch.epfl.data.squall.utilities.SystemParameters;

public class PullStatisticCollector {
    private static Logger LOG = Logger.getLogger(PullStatisticCollector.class);

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

    private static List<List<String>> readTuples(CustomReader reader,
	    ChainOperator operators, Map map) {
	String line = null;
	List<String> tuple = null;
	List<List<String>> result = new ArrayList<List<String>>();
	while (result.size() == 0 && (line = readLine(reader)) != null) {
	    tuple = MyUtilities.fileLineToTuple(line, map);
	    result = operators.process(tuple, -1);
	}
	return result;
    }

    private static <T extends Comparable<T>> List<T> readTuplesJoinKey(
	    CustomReader reader, ChainOperator operators, Type<T> conv, Map map) {
	List<List<String>> tuples = readTuples(reader, operators, map);
        List<T> result = new ArrayList<T>();
        for (List<String> tuple : tuples) {
          result.add(conv.fromString(tuple.get(0)));
        }
	return result;
    }

    private static <T extends Comparable<T>> List<T> readAllTupleJoinKeys(
	    CustomReader reader, ChainOperator operators, Type<T> conv, Map map) {
	List<T> tupleKeys = new ArrayList<T>();
        List<T> tuples = readTuplesJoinKey(reader, operators, conv, map);

        while (tuples.size() > 0) {
          for (T key : tuples) {
            tupleKeys.add(key);
          }
          tuples = readTuplesJoinKey(reader, operators, conv, map);
        }

	return tupleKeys;
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

    private static void printWallClockTime() {
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	Calendar cal = Calendar.getInstance();
	LOG.info(dateFormat.format(cal.getTime()));
    }

    private void processConfig(Map map) {
	printWallClockTime();
	long startTime = System.currentTimeMillis();

	// generate matrix
	PLCQueryPlan generator = chooseQueryPlan(map);
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

    public static void main(String[] args) {
	new PullStatisticCollector(args);
    }

    // change from here down
    private PLCQueryPlan chooseQueryPlan(Map map) {
	String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
	PLCQueryPlan generator = null;
	if (queryName.equalsIgnoreCase("hyracks")) {
	    generator = new Hyracks(map);
	} else {
	    throw new RuntimeException("Unsupported query plan " + queryName
		    + "!");
	}
	return generator;
    }

    private static interface PLCQueryPlan {
	public JoinMatrix generateMatrix();
    }

    private static class Hyracks implements PLCQueryPlan {
	private IntegerType _ic = new IntegerType();

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
}
