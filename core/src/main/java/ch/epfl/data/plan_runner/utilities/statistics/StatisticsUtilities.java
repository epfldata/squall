package ch.epfl.data.plan_runner.utilities.statistics;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

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
