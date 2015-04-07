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


package ch.epfl.data.squall.utilities.statistics;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.utilities.SystemParameters;

public class StatisticsUtilities implements Serializable {
	public static double bytesToMegabytes(long bytes) {
		return bytes / 1024;
	}

	private static final long serialVersionUID = 1L;

	private final int STATS_TEST = 1; // 1-> memoryTest & output Test anything
	// Else->time Test
	private final int isTest = 1;

	private final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	// SET PARAMS
	// public static String MemoryTestComponentID ="LINEITEM_SUPPLIER_NATION2";
	private long dipInputFreqPrint = 100000;

	private long dipOutputFreqPrint = 2000000;

	public StatisticsUtilities(Map conf, Logger LOG) {
		if (SystemParameters.isExisting(conf, "DIP_INPUT_FREQ_PRINT")) {
			dipInputFreqPrint = SystemParameters.getInt(conf,
					"DIP_INPUT_FREQ_PRINT");
			LOG.info("Setting MemoryTestNumTuples to " + dipInputFreqPrint);
		}
		if (SystemParameters.isExisting(conf, "DIP_OUTPUT_FREQ_PRINT")) {
			dipOutputFreqPrint = SystemParameters.getInt(conf,
					"DIP_OUTPUT_FREQ_PRINT");
			LOG.info("Setting OutputTestNumTuples to " + dipOutputFreqPrint);
		}
	}

	public long getDipInputFreqPrint() {
		return dipInputFreqPrint;
	}

	public long getDipOutputFreqPrint() {
		return dipOutputFreqPrint;
	}

	public boolean isTestMode() {
		return STATS_TEST == isTest;
	}

	public void printInitialStats(Logger LOG, int thisTaskID,
			int firstRelationSize, int firstTaggedRelationSize,
			int secondRelationSize, int secondTaggedRelationSize) {
		final Runtime runtime = Runtime.getRuntime();
		final long memory = runtime.totalMemory() - runtime.freeMemory();
		LOG.info(","
				+ "INITIAL,"
				+ thisTaskID
				+ ",:"
				+ "TimeStamp:,"
				+ dateFormat.format(Calendar.getInstance().getTime())
				+ ", FirstStorage:,"
				+ (firstRelationSize + firstTaggedRelationSize)
				+ ", SecondStorage:,"
				+ (secondRelationSize + secondTaggedRelationSize)
				+ ", Total:,"
				+ (firstRelationSize + secondRelationSize
						+ firstTaggedRelationSize + secondTaggedRelationSize)
				+ ", Memory used: ," + bytesToMegabytes(memory) + ","
				+ bytesToMegabytes(runtime.totalMemory()));
	}

	public void printMemoryStats(int heirarchyPosition, Logger LOG,
			int thisTaskID, long numberOfTuplesMemory, int firstRelationSize,
			int firstTaggedRelationSize, int secondRelationSize,
			int secondTaggedRelationSize) {
		if (STATS_TEST == isTest)
			if (heirarchyPosition == StormComponent.FINAL_COMPONENT) {
				final Runtime runtime = Runtime.getRuntime();
				final long memory = runtime.totalMemory()
						- runtime.freeMemory();
				LOG.info(","
						+ "MEMORY,"
						+ thisTaskID
						+ ",:"
						+ "TimeStamp:,"
						+ dateFormat.format(Calendar.getInstance().getTime())
						+ ", FirstStorage:,"
						+ (firstRelationSize + firstTaggedRelationSize)
						+ ", SecondStorage:,"
						+ (secondRelationSize + secondTaggedRelationSize)
						+ ", Total:,"
						+ (numberOfTuplesMemory)
						+ ", Memory used: ,"
						+ StatisticsUtilities.bytesToMegabytes(memory)
						+ ","
						+ StatisticsUtilities.bytesToMegabytes(runtime
								.totalMemory()));
			}
	}

	public void printResultStats(int heirarchyPosition, Logger LOG,
			long numTuplesOutputted, int thisTaskID, boolean isPrintAnyway) {
		if (STATS_TEST == isTest
				&& (numTuplesOutputted % dipOutputFreqPrint == 0 || isPrintAnyway))
			if (heirarchyPosition == StormComponent.FINAL_COMPONENT)
				LOG.info("," + "RESULT," + thisTaskID + "," + "TimeStamp:,"
						+ dateFormat.format(Calendar.getInstance().getTime())
						+ ",Sent Tuples," + numTuplesOutputted);
	}

}
