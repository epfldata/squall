package plan_runner.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.storm_components.InterchangingComponent;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.StormSrcHarmonizer;
import plan_runner.thetajoin.matrix_mapping.MatrixAssignment;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyUtilities {
	private static Logger LOG = Logger.getLogger(MyUtilities.class);

	public static final String SINGLE_HASH_KEY = "SingleHashEntry";

	public static InputDeclarer attachEmitterBatch(Map map, List<String> fullHashList,
			InputDeclarer currentBolt, StormEmitter emitter1, StormEmitter... emittersArray) {
		final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.addAll(Arrays.asList(emittersArray));

		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				currentBolt = currentBolt.customGrouping(emitterID, new BatchStreamGrouping(map,
						fullHashList));
		}
		return currentBolt;
	}

	public static InputDeclarer attachEmitterComponentsReshuffled(InputDeclarer currentBolt,
			StormEmitter emitter1, StormEmitter... emittersArray) {
		final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.addAll(Arrays.asList(emittersArray));

		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				currentBolt = currentBolt.shuffleGrouping(emitterID);
		}
		return currentBolt;
	}

	public static InputDeclarer attachEmitterHash(Map map, List<String> fullHashList,
			InputDeclarer currentBolt, StormEmitter emitter1, StormEmitter... emittersArray) {
		final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.addAll(Arrays.asList(emittersArray));

		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				currentBolt = currentBolt.customGrouping(emitterID, new HashStreamGrouping(map,
						fullHashList));
		}
		return currentBolt;
	}

	public static void checkBatchOutput(long batchOutputMillis, AggregateOperator aggregation,
			Map conf) {
		if (aggregation == null && batchOutputMillis != 0)
			throw new RuntimeException(
					"A component must have aggregation operator in order to support batching.");
		if (isAckEveryTuple(conf) && batchOutputMillis != 0)
			throw new RuntimeException("With batching, only AckAtEnd mode is allowed!");
		// we don't keep Storm Tuple instances for batched tuples
		// we also ack them immediately, which doesn't fir in AckEveryTime
		// logic
	}

	public static boolean checkSendMode(Map map) {
		if (SystemParameters.isExisting(map, "BATCH_SEND_MODE")) {
			final String mode = SystemParameters.getString(map, "BATCH_SEND_MODE");
			if (!mode.equalsIgnoreCase("THROTTLING") && !mode.equalsIgnoreCase("SEND_AND_WAIT")
					&& !mode.equalsIgnoreCase("MANUAL_BATCH"))
				return false;
		}
		return true;
	}

	public static int chooseBalancedTargetIndex(String hash, List<String> allHashes,
			int targetParallelism) {
		return allHashes.indexOf(hash) % targetParallelism;
	}

	public static int chooseHashTargetIndex(String hash, int targetParallelism) {
		return Math.abs(hash.hashCode()) % targetParallelism;
	}

	public static String createHashString(List<String> tuple, List<Integer> hashIndexes,
			List<ValueExpression> hashExpressions, Map map) {
		if (hashIndexes == null && hashExpressions == null)
			return SINGLE_HASH_KEY;

		final String columnDelimiter = getColumnDelimiter(map);

		// NOTE THAT THE HASHCOLUMN depend upon the output of the projection!!
		final StringBuilder hashStrBuf = new StringBuilder();
		if (hashIndexes != null)
			for (final int hashIndex : hashIndexes)
				hashStrBuf.append(tuple.get(hashIndex)).append(columnDelimiter);
		if (hashExpressions != null)
			for (final ValueExpression hashExpression : hashExpressions)
				hashStrBuf.append(hashExpression.eval(tuple)).append(columnDelimiter);

		// remove one extra HASH_DELIMITER at the end

		final int hdLength = columnDelimiter.length();
		final int fullLength = hashStrBuf.length();
		return hashStrBuf.substring(0, fullLength - hdLength);

	}

	public static String createHashString(List<String> tuple, List<Integer> hashIndexes, Map map) {
		if (hashIndexes == null || hashIndexes.isEmpty())
			return SINGLE_HASH_KEY;
		String hashString = "";
		final int tupleLength = hashIndexes.size();
		for (int i = 0; i < tupleLength; i++)
			// depend upon the output of the
			// projection!!
			if (i == tupleLength - 1)
				hashString += tuple.get(hashIndexes.get(i));
			else
				hashString += tuple.get(hashIndexes.get(i)) + getColumnDelimiter(map);
		return hashString;
	}

	public static List<String> createOutputTuple(List<String> firstTuple, List<String> secondTuple) {
		final List<String> outputTuple = new ArrayList<String>();

		for (int j = 0; j < firstTuple.size(); j++)
			// first relation (R)
			outputTuple.add(firstTuple.get(j));
		for (int j = 0; j < secondTuple.size(); j++)
			outputTuple.add(secondTuple.get(j));
		return outputTuple;
	}

	public static List<String> createOutputTuple(List<String> firstTuple, List<String> secondTuple,
			List<Integer> joinParams) {
		final List<String> outputTuple = new ArrayList<String>();

		for (int j = 0; j < firstTuple.size(); j++)
			// first relation (R)
			outputTuple.add(firstTuple.get(j));
		for (int j = 0; j < secondTuple.size(); j++)
			if ((joinParams == null) || (!joinParams.contains(j)))
				// not
				// exits
				// add
				// the
				// column!!
				// (S)
				outputTuple.add(secondTuple.get(j));
		return outputTuple;
	}

	public static Values createTupleValues(List<String> tuple, long timestamp,
			String componentIndex, List<Integer> hashIndexes,
			List<ValueExpression> hashExpressions, Map conf) {

		final String outputTupleHash = MyUtilities.createHashString(tuple, hashIndexes,
				hashExpressions, conf);
		if (MyUtilities.isCustomTimestampMode(conf))
			return new Values(componentIndex, tuple, outputTupleHash, timestamp);
		else
			return new Values(componentIndex, tuple, outputTupleHash);
	}

	public static Values createUniversalFinalAckTuple(Map map) {
		final Values values = new Values();
		values.add("N/A");
		if (!MyUtilities.isManualBatchingMode(map)) {
			final List<String> lastTuple = new ArrayList<String>(
					Arrays.asList(SystemParameters.LAST_ACK));
			values.add(lastTuple);
			values.add("N/A");
		} else
			values.add(SystemParameters.LAST_ACK);
		if (MyUtilities.isCustomTimestampMode(map))
			values.add(0);
		return values;
	}

	public static void dumpSignal(StormComponent comp, Tuple stormTupleRcv,
			OutputCollector collector) {
		comp.printContent();
		collector.ack(stormTupleRcv);
	}

	/*
	 * Different tuple<->(String, Hash) conversions
	 */
	public static List<String> fileLineToTuple(String line, Map conf) {
		final String[] columnValues = line.split(SystemParameters.getString(conf,
				"DIP_READ_SPLIT_DELIMITER"));
		return new ArrayList<String>(Arrays.asList(columnValues));
	}

	/*
	 * For each emitter component (there are two input emitters for each join),
	 * appropriately connect with all of its inner Components that emits tuples
	 * to StormDestinationJoin. For destinationJoiner, there is only one bolt
	 * that emits tuples, but for sourceJoiner, there are two SourceStorage (one
	 * for storing each emitter tuples), which emits tuples.
	 */
	/*
	 * public static InputDeclarer attachEmitterComponents(InputDeclarer
	 * currentBolt, StormEmitter emitter1, StormEmitter... emittersArray){
	 * List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	 * emittersList.add(emitter1);
	 * emittersList.addAll(Arrays.asList(emittersArray));
	 * for(StormEmitter emitter: emittersList){ String[] emitterIDs =
	 * emitter.getEmitterIDs(); for(String emitterID: emitterIDs){ currentBolt =
	 * currentBolt.fieldsGrouping(emitterID, new Fields("Hash")); } } return
	 * currentBolt; }
	 */

	// collects all the task ids for "default" stream id
	public static List<Integer> findTargetTaskIds(TopologyContext tc) {
		final List<Integer> result = new ArrayList<Integer>();
		final Map<String, Map<String, Grouping>> streamComponentGroup = tc.getThisTargets();
		final Iterator<Entry<String, Map<String, Grouping>>> it = streamComponentGroup.entrySet()
				.iterator();
		while (it.hasNext()) {
			final Map.Entry<String, Map<String, Grouping>> pair = it.next();
			final String streamId = pair.getKey();
			final Map<String, Grouping> componentGroup = pair.getValue();
			if (streamId.equalsIgnoreCase("default")) {
				final Iterator<Entry<String, Grouping>> innerIt = componentGroup.entrySet()
						.iterator();
				while (innerIt.hasNext()) {
					final Map.Entry<String, Grouping> innerPair = innerIt.next();
					final String componentId = innerPair.getKey();
					// Grouping group = innerPair.getValue();
					// if (group.is_set_direct()){
					result.addAll(tc.getComponentTasks(componentId));
					// }
				}
			}
		}
		return result;
	}

	// Previously HASH_DELIMITER = "-" in SystemParameters, but now is the same
	// as DIP_GLOBAL_ADD_DELIMITER
	// we need it for preaggregation
	public static String getColumnDelimiter(Map map) {
		return SystemParameters.getString(map, "DIP_GLOBAL_ADD_DELIMITER");
	}

	public static int getCompBatchSize(String compName, Map map) {
		return SystemParameters.getInt(map, compName + "_BS");
	}

	public static TypeConversion getDominantNumericType(List<ValueExpression> veList) {
		TypeConversion wrapper = veList.get(0).getType();
		for (int i = 1; i < veList.size(); i++) {
			final TypeConversion currentType = veList.get(1).getType();
			if (isDominant(currentType, wrapper))
				wrapper = currentType;
		}
		return wrapper;
	}

	public static long getMin(long first, long second) {
		return first < second ? first : second;
	}

	public static int getNumParentTasks(TopologyContext tc, List<StormEmitter> emittersList) {
		int result = 0;
		for (final StormEmitter emitter : emittersList) {
			// We have multiple emitterIDs only for StormSrcJoin
			final String[] ids = emitter.getEmitterIDs();
			for (final String id : ids)
				result += tc.getComponentTasks(id).size();
		}
		return result;
	}

	// used for NoACK optimization
	public static int getNumParentTasks(TopologyContext tc, StormEmitter emitter1,
			StormEmitter... emittersArray) {
		final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.addAll(Arrays.asList(emittersArray));

		return getNumParentTasks(tc, emittersList);
	}

	// used for NoACK optimization for StormSrcJoin
	public static int getNumParentTasks(TopologyContext tc, StormSrcHarmonizer harmonizer) {
		final String id = String.valueOf(harmonizer.getID());
		return tc.getComponentTasks(String.valueOf(id)).size();
	}

	/*
	 * Method invoked with arguments "a/b//c/e//f", 0 return "f" Method invoked
	 * with arguments "a/b//c/e//f", 1 return "e"
	 */
	public static String getPartFromEnd(String path, int fromEnd) {
		final String parts[] = path.split("\\/+");
		final int length = parts.length;
		return parts[length - (fromEnd + 1)];
	}

	public static String getStackTrace(Throwable aThrowable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return result.toString();
	}

	// if this is false, we have a specific mechanism to ensure all the tuples
	// are fully processed
	// it is based on CustomStreamGrouping
	public static boolean isAckEveryTuple(Map map) {
		int ackers;
		if (!SystemParameters.isExisting(map, "DIP_NUM_ACKERS"))
			// number of ackers is defined in storm.yaml
			ackers = SystemParameters.DEFAULT_NUM_ACKERS;
		else
			ackers = SystemParameters.getInt(map, "DIP_NUM_ACKERS");
		return (ackers > 0);
	}

	public static boolean isAggBatchOutputMode(long batchOutputMillis) {
		return batchOutputMillis != 0L;
	}

	public static boolean isCustomTimestampMode(Map map) {
		return SystemParameters.isExisting(map, "CUSTOM_TIMESTAMP")
				&& SystemParameters.getBoolean(map, "CUSTOM_TIMESTAMP");
	}
	
	public static boolean isStatisticsCollector(Map map, int hierarchyPosition) {
		return hierarchyPosition == StormComponent.FINAL_COMPONENT
				&& SystemParameters.isExisting(map, "DIP_STATISTIC_COLLECTOR")
				&& SystemParameters.getBoolean(map, "DIP_STATISTIC_COLLECTOR");
	}

	/*
	 * Does bigger dominates over smaller? For (bigger, smaller) = (double,
	 * long) answer is yes.
	 */
	private static boolean isDominant(TypeConversion bigger, TypeConversion smaller) {
		// for now we only have two numeric types: double and long
		if (bigger instanceof DoubleConversion)
			return true;
		else
			return false;
	}

	public static boolean isFinalAck(List<String> tuple, Map map) {
		return (!isAckEveryTuple(map)) && isFinalAck(tuple.get(0));
	}

	private static boolean isFinalAck(String tupleString) {
		return tupleString.equals(SystemParameters.LAST_ACK);
	}

	public static boolean isFinalAckManualBatching(String tupleString, Map map) {
		return (!isAckEveryTuple(map)) && isFinalAck(tupleString);
	}

	public static boolean isManualBatchingMode(Map map) {
		return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
				&& SystemParameters.getString(map, "BATCH_SEND_MODE").equalsIgnoreCase(
						"MANUAL_BATCH");
	}

	public static boolean isPrintLatency(int hierarchyPosition, Map conf) {
		return MyUtilities.isCustomTimestampMode(conf)
				&& hierarchyPosition == StormComponent.FINAL_COMPONENT;
	}

	public static boolean isSending(int hierarchyPosition, long batchOutputMillis) {
		return (hierarchyPosition != StormComponent.FINAL_COMPONENT)
				&& !isAggBatchOutputMode(batchOutputMillis);
	}

	public static boolean isStoreTimestamp(Map map, int hierarchyPosition) {
		return isCustomTimestampMode(map) && hierarchyPosition == StormComponent.FINAL_COMPONENT
				&& SystemParameters.isExisting(map, "STORE_TIMESTAMP")
				&& SystemParameters.getBoolean(map, "STORE_TIMESTAMP");
	}

	public static boolean isThrottlingMode(Map map) {
		return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
				&& SystemParameters.getString(map, "BATCH_SEND_MODE")
						.equalsIgnoreCase("THROTTLING");
	}

	public static <T extends Comparable<T>> List<ValueExpression> listTypeErasure(
			List<ValueExpression<T>> input) {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		for (final ValueExpression ve : input)
			result.add(ve);
		return result;
	}

	public static void printBlockingResult(String componentName, AggregateOperator agg,
			int hierarchyPosition, Map map, Logger log) {
		// just print it, necessary for both modes (in Local mode we might print
		// other than final components)
		printPartialResult(componentName, agg.getNumTuplesProcessed(), agg.printContent(), map, log);

		LocalMergeResults.localCollectFinalResult(agg, hierarchyPosition, map, log);
	}

	// this method is called when the last operator is not an aggregateOperator
	public static void printBlockingResult(String componentName, int numProcessedTuples,
			String compContent, int hierarchyPosition, Map map, Logger log) {
		// just print it, necessary for both modes (in Local mode we might print
		// other than final components)
		printPartialResult(componentName, numProcessedTuples, compContent, map, log);
	}

	private static void printPartialResult(String componentName, int numProcessedTuples,
			String compContent, Map map, Logger log) {
		final StringBuilder sb = new StringBuilder();
		sb.append("\nThe result for topology ");
		sb.append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
		sb.append("\nComponent ").append(componentName).append(":\n");
		sb.append("\nThis task received ").append(numProcessedTuples);
		sb.append("\n").append(compContent);
		log.info(sb.toString());
	}

	// in ProcessFinalAck and dumpSignal we have acking at the end, because we
	// return after that
	public static void processFinalAck(int numRemainingParents, int hierarchyPosition, Map conf,
			Tuple stormTupleRcv, OutputCollector collector) {
		if (numRemainingParents == 0)
			// this task received from all the parent tasks
			// SystemParameters.LAST_ACK
			if (hierarchyPosition != StormComponent.FINAL_COMPONENT) {
				// if this component is not the last one
				final Values values = createUniversalFinalAckTuple(conf);
				collector.emit(values);
			} else
				collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
		collector.ack(stormTupleRcv);
	}

	public static void processFinalAck(int numRemainingParents, int hierarchyPosition, Map conf,
			Tuple stormTupleRcv, OutputCollector collector, PeriodicAggBatchSend periodicBatch) {
		if (numRemainingParents == 0)
			if (periodicBatch != null) {
				periodicBatch.cancel();
				periodicBatch.getComponent().aggBatchSend();
			}
		processFinalAck(numRemainingParents, hierarchyPosition, conf, stormTupleRcv, collector);
	}

	/*
	 * Read query plans - read as verbatim
	 */
	public static String readFile(String path) {
		try {
			final StringBuilder sb = new StringBuilder();
			final BufferedReader reader = new BufferedReader(new FileReader(new File(path)));

			String line;
			while ((line = reader.readLine()) != null)
				sb.append(line).append("\n");
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1); // last \n is unnecessary
			reader.close();

			return sb.toString();
		} catch (final IOException ex) {
			final String err = MyUtilities.getStackTrace(ex);
			throw new RuntimeException("Error while reading a file:\n " + err);
		}
	}

	/*
	 * Used for reading a result file, # should be treated as possible data, not
	 * comment
	 */
	public static List<String> readFileLinesSkipEmpty(String path) throws IOException {
		final BufferedReader reader = new BufferedReader(new FileReader(new File(path)));

		final List<String> lines = new ArrayList<String>();
		String strLine;
		while ((strLine = reader.readLine()) != null)
			if (!strLine.isEmpty())
				lines.add(strLine);
		reader.close();
		return lines;
	}

	/*
	 * Used for reading an SQL file
	 */
	public static String readFileSkipEmptyAndComments(String path) {
		try {
			final StringBuilder sb = new StringBuilder();

			final List<String> lines = readFileLinesSkipEmpty(path);
			for (String line : lines) {
				line = line.trim();
				if (!line.startsWith("#"))
					sb.append(line).append(" ");
			}
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1); // last space is unnecessary

			return sb.toString();
		} catch (final IOException ex) {
			final String err = MyUtilities.getStackTrace(ex);
			throw new RuntimeException("Error while reading a file:\n " + err);
		}
	}

	// this is for Spout
	public static void sendTuple(Values stormTupleSnd, SpoutOutputCollector collector, Map conf) {
		String msgId = null;
		if (MyUtilities.isAckEveryTuple(conf))
			msgId = "T"; // as short as possible

		if (msgId != null)
			collector.emit(stormTupleSnd, msgId);
		else
			collector.emit(stormTupleSnd);
	}

	/*
	 * no acking at the end, because for one tuple arrived in JoinComponent, we
	 * might have multiple tuples to be sent.
	 */
	public static void sendTuple(Values stormTupleSnd, Tuple stormTupleRcv,
			OutputCollector collector, Map conf) {
		// stormTupleRcv is equals to null when we send tuples in batch fashion
		if (isAckEveryTuple(conf) && stormTupleRcv != null)
			collector.emit(stormTupleRcv, stormTupleSnd);
		else
			collector.emit(stormTupleSnd);
	}

	public static List<String> stringToTuple(String tupleString, Map conf) { // arraylist
		// 2
		// values
		final String[] columnValues = tupleString.split(SystemParameters.getString(conf,
				"DIP_GLOBAL_SPLIT_DELIMITER"));
		return new ArrayList<String>(Arrays.asList(columnValues));
	}

	public static InputDeclarer thetaAttachEmitterComponents(InputDeclarer currentBolt,
			StormEmitter emitter1, StormEmitter emitter2, List<String> allCompNames,
			MatrixAssignment assignment, Map map) {

		// MatrixAssignment assignment = new MatrixAssignment(firstRelationSize,
		// secondRelationSize, parallelism,-1);

		final String firstEmitterIndex = String.valueOf(allCompNames.indexOf(emitter1.getName()));
		final String secondEmitterIndex = String.valueOf(allCompNames.indexOf(emitter2.getName()));

		final ThetaJoinStaticMapping mapping = new ThetaJoinStaticMapping(firstEmitterIndex,
				secondEmitterIndex, assignment, map);

		final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.add(emitter2);

		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				currentBolt = currentBolt.customGrouping(emitterID, mapping);
		}
		return currentBolt;
	}

	public static InputDeclarer thetaAttachEmitterComponentsWithInterChanging(
			InputDeclarer currentBolt, StormEmitter emitter1, StormEmitter emitter2,
			List<String> allCompNames, MatrixAssignment assignment, Map map,
			InterchangingComponent inter) {

		// MatrixAssignment assignment = new MatrixAssignment(firstRelationSize,
		// secondRelationSize, parallelism,-1);

		final String firstEmitterIndex = String.valueOf(allCompNames.indexOf(emitter1.getName()));
		final String secondEmitterIndex = String.valueOf(allCompNames.indexOf(emitter2.getName()));

		final ThetaJoinStaticMapping mapping = new ThetaJoinStaticMapping(firstEmitterIndex,
				secondEmitterIndex, assignment, map);

		final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
		emittersList.add(inter);

		for (final StormEmitter emitter : emittersList) {
			final String[] emitterIDs = emitter.getEmitterIDs();
			for (final String emitterID : emitterIDs)
				currentBolt = currentBolt.customGrouping(emitterID, mapping);
		}
		return currentBolt;
	}

	public static String tupleToString(List<String> tuple, Map conf) {
		String tupleString = "";
		for (int i = 0; i < tuple.size(); i++)
			if (i == tuple.size() - 1)
				tupleString += tuple.get(i);
			else
				tupleString += tuple.get(i)
						+ SystemParameters.getString(conf, "DIP_GLOBAL_ADD_DELIMITER");
		// this cause a bug when space (" ") is the last character:
		// tupleString=tupleString.trim();
		return tupleString;
	}
	
	public static boolean isBDB(Map conf){
		return SystemParameters.isExisting(conf, "DIP_IS_BDB")
			&& SystemParameters.getBoolean(conf, "DIP_IS_BDB");
	}
	
	public static boolean isBDBUniform(Map conf) {
		return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
				&& SystemParameters.getString(conf, "DIP_BDB_TYPE").equalsIgnoreCase("UNIFORM");	
	}

	public static boolean isBDBSkewed(Map conf) {
		return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
				&& SystemParameters.getString(conf, "DIP_BDB_TYPE").equalsIgnoreCase("SKEWED");
	}

	public static List<String> listFilesForPath(String dir) {
		List<String> filePaths = new ArrayList<String>();

		File folder = new File(dir);
		for(File fileEntry: folder.listFiles()){
			if(fileEntry.isDirectory()){
				if(!fileEntry.getName().startsWith(".")){
					// avoid hidden folder
					filePaths.addAll(listFilesForPath(fileEntry.getAbsolutePath()));
				}
			}else{
				filePaths.add(fileEntry.getAbsolutePath());
			}
		}
		
		return filePaths;
	}
	
}