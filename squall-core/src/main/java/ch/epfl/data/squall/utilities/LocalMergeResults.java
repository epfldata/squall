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

package ch.epfl.data.squall.utilities;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

//import frontend.functional.scala.operators.ScalaAggregateOperator;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateAvgOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.types.Type;

public class LocalMergeResults {
    private static Logger LOG = Logger.getLogger(LocalMergeResults.class);

    // for writing the full final result in Local Mode
    private static int _collectedLastComponents = 0;

    private static int _numTuplesProcessed = 0;
    // the number of tuples the componentTask is reponsible for (!! not how many
    // tuples are in storage!!)

    private static AggregateOperator _computedAgg;

    private static AggregateOperator _fileAgg;

    private static Semaphore _semFullResult = new Semaphore(1, true);
    private static Semaphore _semNumResults = new Semaphore(0, true);


  public static void reset() {
    _collectedLastComponents = 0;
    _numTuplesProcessed = 0;
    _computedAgg = null;
    _fileAgg = null;
    _semFullResult = new Semaphore(1, true);
    _semNumResults = new Semaphore(0, true);
  }

    private static void addMoreResults(AggregateOperator lastAgg, Map map) {
	if (_computedAgg == null) {
	    // first task of the last component asked to be added
	    // we create empty aggregations, which we later fill, one from
	    // tasks, other from a file
	    _computedAgg = createOverallAgg(lastAgg, map);
	    _fileAgg = (AggregateOperator) DeepCopy.copy(_computedAgg);
	    fillAggFromResultFile(map);
	}

	if (_computedAgg.getStorage() instanceof AggregationStorage) {
	    AggregationStorage stor = (AggregationStorage) _computedAgg
		    .getStorage();
	    stor.addContent((AggregationStorage) (lastAgg.getStorage()));
	}
	/*
	 * if (_computedAgg.getStorage() instanceof WindowAggregationStorage) {
	 * WindowAggregationStorage stor = (WindowAggregationStorage)
	 * _computedAgg .getStorage();
	 * stor.addContent((WindowAggregationStorage) (lastAgg.getStorage())); }
	 */
    }

    private static AggregateOperator createOverallAgg(
	    AggregateOperator lastAgg, Map map) {
	final Type wrapper = lastAgg.getType();
	AggregateOperator overallAgg;

	ColumnReference cr;
	if (lastAgg.hasGroupBy())
	    cr = new ColumnReference(wrapper, 1);
	else
	    cr = new ColumnReference(wrapper, 0);

	int[] wsMetaData = lastAgg.getWindowSemanticsInfo();

	if (lastAgg instanceof AggregateAvgOperator) {
	    overallAgg = new AggregateAvgOperator(cr, map);
	    if (wsMetaData[0] > 0)
		overallAgg.SetWindowSemantics(wsMetaData[0], wsMetaData[1]);
	}
	/*
	 * else if(lastAgg instanceof ScalaAggregateOperator ){ overallAgg =
	 * ((ScalaAggregateOperator) lastAgg).getNewInstance(); }
	 */
	else {
	    overallAgg = new AggregateSumOperator(cr, map);
	    if (wsMetaData[0] > 0)
		overallAgg.SetWindowSemantics(wsMetaData[0], wsMetaData[1]);
	}

	if (lastAgg.hasGroupBy())
	    overallAgg.setGroupByColumns(Arrays.asList(0));

	return overallAgg;
    }

    private static void fillAggFromResultFile(Map map) {
	try {
	    final String path = getResultFilePath(map);
	    final List<String> lines = MyUtilities.readFileLinesSkipEmpty(path);

	    for (final String line : lines) {
		// List<String> tuple = Arrays.asList(line.split("\\s+=\\s+"));
		// we want to catch exactly one space between and after =.
		// tuple might consist of spaces as well
		final List<String> tuple = Arrays.asList(line.split(" = "));
		_fileAgg.process(tuple, -1);
	    }
	} catch (final IOException ex) {
	    // problem with finding the result file
	    _fileAgg = null;
	}
    }

    // getting size information - from path "../test/data/tpch/0.01G",
    // it extracts dataSize = 0.01G
    // For Squall (not in Squall Plan Runner) there is DIP_DB_SIZE,
    // but this method has to be used for PlanRunner as well.
    private static String getDataSizeInfo(Map map) {
	final String path = SystemParameters.getString(map, "DIP_DATA_PATH");
	return MyUtilities.getPartFromEnd(path, 0);
    }

    // this has to be a separate method, because we don't want Exception if
    // DIP_RESULT_ROOT is not set
    private static String getResultDir(Map map) {
	String resultRoot = "";
	if (SystemParameters.isExisting(map, "DIP_RESULT_ROOT"))
	    resultRoot = SystemParameters.getString(map, "DIP_RESULT_ROOT");
	return resultRoot;
    }

    public static String getResultFilePath(Map map) {
	final String rootDir = getResultDir(map);
	final String schemaName = getSchemaName(map);
	final String dataSize = getDataSizeInfo(map);
	final String queryName = SystemParameters.getString(map,
		"DIP_QUERY_NAME");
	return rootDir + "/" + schemaName + "/" + dataSize + "/" + queryName
		+ ".result";
    }

    /*
     * from "../test/data/tpch/0.01G" as dataPath, return tpch
     */
    private static String getSchemaName(Map map) {
	final String path = SystemParameters.getString(map, "DIP_DATA_PATH");
	return MyUtilities.getPartFromEnd(path, 1);
    }

    // The following 2 methods are crucial for collecting, printing and
    // comparing the results in Local Mode
    // called on the component task level, when all Spouts fully propagated
    // their tuples
    public static void localCollectFinalResult(AggregateOperator lastAgg,
	    int hierarchyPosition, Map map, Logger log) {
	if ((!SystemParameters.getBoolean(map, "DIP_DISTRIBUTED"))
		&& hierarchyPosition == StormComponent.FINAL_COMPONENT)
	    try {
		// prepare it for printing at the end of the execution
		_semFullResult.acquire();

		_collectedLastComponents++;
		_numTuplesProcessed += lastAgg.getNumTuplesProcessed();
		addMoreResults(lastAgg, map);

		_semFullResult.release();
		_semNumResults.release();
	    } catch (final InterruptedException ex) {
		throw new RuntimeException(
			"InterruptedException unexpectedly occured!");
	    }
    }

    private static int localCompare(Map map) {
	if (_fileAgg == null) {
	    LOG.info("\nCannot validate the result, result file "
		    + getResultFilePath(map) + " does not exist."
		    + "\n  Make sure you specified correct DIP_RESULT_ROOT and"
		    + "\n  created result file with correct name.");
	    return 1;
	}
	if (_computedAgg.getStorage().equals(_fileAgg.getStorage())) {
	    LOG.info("\nOK: Expected result achieved for "
		    + SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
	    return 0;
	} else {
	    final StringBuilder sb = new StringBuilder();
	    sb.append("\nPROBLEM: Not expected result achieved for ").append(
		    SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
	    sb.append("\nCOMPUTED: \n").append(_computedAgg.printContent());
	    sb.append("\nFROM THE RESULT FILE: \n").append(
		    _fileAgg.printContent());
	    LOG.info(sb.toString());
	    return 1;
	}
    }

    private static void localPrint(String finalResult, Map map) {
	final StringBuilder sb = new StringBuilder();
	sb.append("\nThe full result for topology ");
	sb.append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME")).append(
		".");
	sb.append("\nCollected from ").append(_collectedLastComponents)
		.append(" component tasks of the last component.");
	sb.append("\nAll the tasks of the last component in total received ")
		.append(_numTuplesProcessed).append(" tuples.");
	sb.append("\n").append(finalResult);
	LOG.info(sb.toString());
    }

    // called just before killExecution
    // only for local mode, since they are executed in a single process, sharing
    // all the classes
    // we need it due to collectedLastComponents, and lines of result
    // in cluster mode, they can communicate only through conf file
    public static int localPrintAndCompare(Map map) {
	if (_computedAgg == null)
	    return -1;
	localPrint(_computedAgg.printContent(), map);
	return localCompare(map);
    }

    public static BasicStore getResults() {
      	return _computedAgg.getStorage();
    }

    public static void waitForResults(int howMany) throws InterruptedException {
      _semNumResults.acquire(howMany);
      //assert(_collectedLastComponents == howMany);
    }
}
