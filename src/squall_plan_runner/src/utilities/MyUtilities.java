package utilities;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import expressions.ValueExpression;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import thetajoin.matrixMapping.MatrixAssignment;
import operators.AggregateOperator;

import org.apache.log4j.Logger;
import stormComponents.StormComponent;
import stormComponents.StormEmitter;
import stormComponents.StormSrcHarmonizer;


public class MyUtilities{
        private static Logger LOG = Logger.getLogger(MyUtilities.class);

        public static final String SINGLE_HASH_KEY = "SingleHashEntry";

        public static String getStackTrace(Throwable aThrowable) {
            final Writer result = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(result);
            aThrowable.printStackTrace(printWriter);
            return result.toString();
        }

        //this method is called when the last operator is not an aggregateOperator
        public static void printBlockingResult(String componentName, 
                                               int numProcessedTuples,
                                               String compContent,
                                               int hierarchyPosition,
                                               Map map,
                                               Logger log){
            //just print it, necessary for both modes (in Local mode we might print other than final components)
            printPartialResult(componentName, numProcessedTuples, compContent, map, log);
        }

        public static void printBlockingResult(String componentName, 
                                               AggregateOperator agg,
                                               int hierarchyPosition,
                                               Map map,
                                               Logger log){
            //just print it, necessary for both modes (in Local mode we might print other than final components)
            printPartialResult(componentName, agg.getNumTuplesProcessed(), agg.printContent(), map, log);

            LocalMergeResults.localCollectFinalResult(agg, hierarchyPosition, map, log);
        }

        private static void printPartialResult(String componentName, 
                                               int numProcessedTuples,
                                               String compContent,
                                               Map map,
                                               Logger log) {
            StringBuilder sb = new StringBuilder();
            sb.append("\nThe result for topology ");
            sb.append(MyUtilities.getFullTopologyName(map));
            sb.append("\nComponent ").append(componentName).append(":\n");
            sb.append("\nThis task received ").append(numProcessedTuples);
            sb.append("\n").append(compContent);
            log.info(sb.toString());
        }

        /*
         * Different tuple<->(String, Hash) conversions
         */

        public static List<String> getLinesFromFile (String path){
            List<String> lines = new ArrayList<String>();
            try {
                FileInputStream fstream = new FileInputStream(path);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                String strLine;
                while ((strLine = br.readLine()) != null)   {
                    if((!strLine.isEmpty()) && (!strLine.startsWith("#"))){
                        lines.add(strLine);
                    }
                }
                in.close();
            } catch (FileNotFoundException ex) {
                LOG.info("\nResult file " + path + " have not be found!");
                lines = null;
            } catch (Exception ex){
                LOG.info(MyUtilities.getStackTrace(ex));
                lines = null;
            }
            return lines;
        }        

        public static List<String> fileLineToTuple(String line, Map conf) {
            String[] columnValues = line.split(SystemParameters.getString(conf, "DIP_READ_SPLIT_DELIMITER"));
            return new ArrayList<String>(Arrays.asList(columnValues));
        }

        public static List<String> stringToTuple(String tupleString, Map conf){  //  arraylist 2 values
            String[] columnValues = tupleString.split(SystemParameters.getString(conf, "DIP_GLOBAL_SPLIT_DELIMITER"));
            return new ArrayList<String>(Arrays.asList(columnValues));
	}

        public static String tupleToString(List<String> tuple, Map conf) {
            String tupleString="";
            for (int i = 0; i < tuple.size(); i++){
		if(i==tuple.size()-1){
                    tupleString+=tuple.get(i);
                } else{
                    tupleString+=tuple.get(i) + SystemParameters.getString(conf, "DIP_GLOBAL_ADD_DELIMITER");
                }
            }
            //this cause a bug when space (" ") is the last character: tupleString=tupleString.trim();
            return tupleString;
        }

        //Previously HASH_DELIMITER = "-" in SystemParameters, but now is the same as DIP_GLOBAL_ADD_DELIMITER
        //we need it for preaggregation
        public static String getHashDelimiter(Map map){
            return SystemParameters.getString(map, "DIP_GLOBAL_ADD_DELIMITER");
        }

        public static String createHashString(List<String> tuple, List<Integer> hashIndexes, Map map) {
            if (hashIndexes == null || hashIndexes.isEmpty()){
                return SINGLE_HASH_KEY;
            }
            String hashString="";
            int tupleLength = hashIndexes.size();
            for (int i = 0; i < tupleLength; i++){   // NOTE THAT THE HASHCOLUMN depend upon the output of the projection!!
		if(i == tupleLength - 1){
                    hashString+=tuple.get(hashIndexes.get(i));
                } else {
                    hashString+=tuple.get(hashIndexes.get(i)) + getHashDelimiter(map);
                }
            }
            return hashString;
        }

        public static String createHashString(List<String> tuple, List<Integer> hashIndexes, List<ValueExpression> hashExpressions, Map map) {
            if (hashIndexes == null && hashExpressions ==null){
                return SINGLE_HASH_KEY;
            }

            String hashDelimiter = getHashDelimiter(map);

            // NOTE THAT THE HASHCOLUMN depend upon the output of the projection!!
            StringBuilder hashStrBuf = new StringBuilder();
            if(hashIndexes != null){
                for(int hashIndex: hashIndexes){
                    hashStrBuf.append(tuple.get(hashIndex)).append(hashDelimiter);
                }
            }
            if(hashExpressions != null){
                for(ValueExpression hashExpression: hashExpressions){
                    hashStrBuf.append(hashExpression.eval(tuple)).append(hashDelimiter);
                }
            }

            //remove one extra HASH_DELIMITER at the end

            int hdLength = hashDelimiter.length();
            int fullLength = hashStrBuf.length();
            return hashStrBuf.substring(0, fullLength - hdLength);

        }

        public static String getFullTopologyName(Map map){
            String topologyName = SystemParameters.getString(map, "DIP_TOPOLOGY_NAME");
            String topologyPrefix = SystemParameters.getString(map, "DIP_TOPOLOGY_NAME_PREFIX");
            if(topologyPrefix != null){
                topologyName = topologyPrefix + "_" + topologyName;
            }
            return topologyName;
        }

    public static List<String> createOutputTuple(List<String> firstTuple, List<String> secondTuple, List<Integer> joinParams) {
        List<String> outputTuple = new ArrayList<String>();

        for (int j = 0; j < firstTuple.size(); j++){ // add all elements of the first relation (R)
            outputTuple.add(firstTuple.get(j));
        }
        for (int j = 0; j < secondTuple.size(); j++) { // now add those
            if((joinParams == null) || (!joinParams.contains(j))){ //if does not exits add the column!! (S)
                outputTuple.add(secondTuple.get(j));
            }
        }
        return outputTuple;
    }

    public static List<String> createOutputTuple(List<String> firstTuple, List<String> secondTuple) {
        List<String> outputTuple = new ArrayList<String>();

        for (int j = 0; j < firstTuple.size(); j++){ // add all elements of the first relation (R)
            outputTuple.add(firstTuple.get(j));
        }
        for (int j = 0; j < secondTuple.size(); j++) { // now add those
            outputTuple.add(secondTuple.get(j));
        }
        return outputTuple;
    }


        /* For each emitter component (there are two input emitters for each join),
         *   appropriately connect with all of its inner Components that emits tuples to StormDestinationJoin.
         * For destinationJoiner, there is only one bolt that emits tuples,
         *   but for sourceJoiner, there are two SourceStorage (one for storing each emitter tuples),
         *   which emits tuples.
         */
        public static InputDeclarer attachEmitterComponents(InputDeclarer currentBolt, 
                StormEmitter emitter1, StormEmitter... emittersArray){
            List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
            emittersList.add(emitter1);
            emittersList.addAll(Arrays.asList(emittersArray));

            for(StormEmitter emitter: emittersList){
                String[] emitterIDs = emitter.getEmitterIDs();
                for(String emitterID: emitterIDs){
                    currentBolt = currentBolt.fieldsGrouping(emitterID, new Fields("Hash"));
                }
            }
            return currentBolt;
        }
        
        public static InputDeclarer thetaAttachEmitterComponents(InputDeclarer currentBolt, 
                StormEmitter emitter1, StormEmitter emitter2,List<String> allCompNames,MatrixAssignment assignment,Map map){
        	
        	//MatrixAssignment assignment = new MatrixAssignment(firstRelationSize, secondRelationSize, parallelism,-1);
        	
        	
        	String firstEmitterIndex = String.valueOf(allCompNames.indexOf(emitter1.getName()));
        	String secondEmitterIndex = String.valueOf(allCompNames.indexOf(emitter2.getName()));
        	
        	ThetaJoinStaticMapping mapping = new ThetaJoinStaticMapping(firstEmitterIndex, secondEmitterIndex, assignment,map);
        	
            ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
            emittersList.add(emitter1);
            emittersList.add(emitter2);

            for(StormEmitter emitter: emittersList){
                String[] emitterIDs = emitter.getEmitterIDs();
                for(String emitterID: emitterIDs){
                    currentBolt = currentBolt.customGrouping(emitterID, mapping);
                }
            }
            return currentBolt;
        }

        public static InputDeclarer attachEmitterCustom(Map map, List<String> fullHashList, InputDeclarer currentBolt,
                StormEmitter emitter1, StormEmitter... emittersArray){
            List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
            emittersList.add(emitter1);
            emittersList.addAll(Arrays.asList(emittersArray));

            for(StormEmitter emitter: emittersList){
                String[] emitterIDs = emitter.getEmitterIDs();
                for(String emitterID: emitterIDs){
                    currentBolt = currentBolt.customGrouping(emitterID,
                            new BalancedStreamGrouping(map, fullHashList));
                }
            }
            return currentBolt;
        }

        public static void checkBatchOutput(long batchOutputMillis, AggregateOperator aggregation, Map conf) {
            if(aggregation == null && batchOutputMillis !=0){
                throw new RuntimeException("A component must have aggregation operator in order to support batching.");
            }
            if(isAckEveryTuple(conf) && batchOutputMillis !=0){
                throw new RuntimeException("With batching, only AckAtEnd mode is allowed!");
                //we don't keep Storm Tuple instances for batched tuples
                //  we also ack them immediately, which doesn't fir in AckEveryTime logic
            }
        }

        //if this is false, we have a specific mechanism to ensure all the tuples are fully processed
        //  it is based on CustomStreamGrouping
        public static boolean isAckEveryTuple(Map map){
            int ackers;
            if(!SystemParameters.isExisting(map, "DIP_NUM_ACKERS")){
                //number of ackers is defined in storm.yaml
                ackers = SystemParameters.DEFAULT_NUM_ACKERS;
            }else{
                ackers = SystemParameters.getInt(map, "DIP_NUM_ACKERS");
            }
            return (ackers > 0);
        }

        public static boolean isFinalAck(List<String> tuple, Map map){
            return (!isAckEveryTuple(map)) && tuple.get(0).equals(SystemParameters.LAST_ACK);
        }

        //in ProcessFinalAck and dumpSignal we have acking at the end, because we return after that
        public static void processFinalAck(int numRemainingParents,
                int hierarchyPosition, Tuple stormTupleRcv, OutputCollector collector) {
            if(numRemainingParents == 0){
            //this task received from all the parent tasks SystemParameters.LAST_ACK
                if(hierarchyPosition != StormComponent.FINAL_COMPONENT){
                //if this component is not the last one
                    List<String> lastTuple = new ArrayList<String>(Arrays.asList(SystemParameters.LAST_ACK));
                    collector.emit(new Values("N/A", lastTuple, "N/A"));
                }else{
                    collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                }
            }
            collector.ack(stormTupleRcv);
        }

        public static void processFinalAck(int numRemainingParents,
                int hierarchyPosition, Tuple stormTupleRcv, OutputCollector collector, PeriodicBatchSend periodicBatch) {
            if(numRemainingParents == 0){
                if(periodicBatch != null){
                    periodicBatch.cancel();
                    periodicBatch.getComponent().batchSend();
                }
            }
            processFinalAck(numRemainingParents, hierarchyPosition, stormTupleRcv, collector);
        }

        public static void dumpSignal(StormComponent comp, Tuple stormTupleRcv, OutputCollector collector) {
            comp.printContent();
            collector.ack(stormTupleRcv);
        }

        public static boolean isBatchOutputMode(long batchOutputMillis) {
            return batchOutputMillis != 0L;
        }

        public static boolean isSending(int hierarchyPosition, long batchOutputMillis) {
            return (hierarchyPosition != StormComponent.FINAL_COMPONENT) && !isBatchOutputMode(batchOutputMillis);
        }

        public static Values createTupleValues(List<String> tuple, String componentIndex,
                List<Integer> hashIndexes, List<ValueExpression> hashExpressions, Map conf) {

            String outputTupleHash = MyUtilities.createHashString(tuple, hashIndexes, hashExpressions, conf);
            return new Values(componentIndex, tuple, outputTupleHash);
        }

        /*
         * no acking at the end, because for one tuple arrived in JoinComponent,
         *   we might have multiple tuples to be sent.
         */
        public static void sendTuple(Values stormTupleSnd, Tuple stormTupleRcv, OutputCollector collector, Map conf) {
            //stormTupleRcv is equals to null when we send tuples in batch fashion
            if(isAckEveryTuple(conf) && stormTupleRcv != null){
                collector.emit(stormTupleRcv, stormTupleSnd);
            }else{
                collector.emit(stormTupleSnd);
            }
        }

        //this is for Spout
        public static void sendTuple(Values stormTupleSnd, SpoutOutputCollector collector, Map conf) {
            String msgId = null;
            if(MyUtilities.isAckEveryTuple(conf)){
                msgId = "TrackTupleAck";
            }

            if(msgId != null){
                collector.emit(stormTupleSnd, msgId);
            }else{
                collector.emit(stormTupleSnd);
            }
        }

        //used for NoACK optimization
        public static int getNumParentTasks(TopologyContext tc,
                StormEmitter emitter1, StormEmitter... emittersArray){
            List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
            emittersList.add(emitter1);
            emittersList.addAll(Arrays.asList(emittersArray));

            int result = 0;
            for(StormEmitter emitter: emittersList){
                //We have multiple emitterIDs only for StormSrcJoin
                String[] ids = emitter.getEmitterIDs();
                for(String id: ids){
                    result += tc.getComponentTasks(id).size();
                }
            }
            return result;
        }

        //used for NoACK optimization for StormSrcJoin
        public static int getNumParentTasks(TopologyContext tc, StormSrcHarmonizer harmonizer){
            String id = String.valueOf(harmonizer.getID());
            return tc.getComponentTasks(String.valueOf(id)).size();
        }

        public static <T extends Comparable<T>> List<ValueExpression> listTypeErasure(List<ValueExpression<T>> input){
            List<ValueExpression> result = new ArrayList<ValueExpression>();
            for(ValueExpression ve: input){
                result.add(ve);
            }
            return result;
        }
	
}