package utilities;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Fields;
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
import operators.AggregateOperator;

import org.apache.log4j.Logger;
import stormComponents.StormEmitter;

public class MyUtilities{
        private static Logger LOG = Logger.getLogger(MyUtilities.class);

	private static int topologyId = 0;
        public static final String SINGLE_HASH_KEY = "SingleHashEntry";

        public static int[] mergeArrays(int[] array1, int[] array2){
            int[] result = new int[array1.length + array2.length];
            System.arraycopy(array1, 0, result, 0, array1.length);
            System.arraycopy(array2, 0, result, array1.length, array2.length);
            return result;
        }

	public static int getNextTopologyId() {
            MyUtilities.topologyId++;
            return MyUtilities.topologyId;
        }

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
            String[] arr= line.split(SystemParameters.getString(conf, "DIP_READ_SPLIT_DELIMITER"));
            List<String> tuple = Arrays.asList(arr);
            return tuple;
        }

        public static List<String> stringToTuple(String tuple, Map conf){  //  arraylist 2 values
            String[] columnValues=tuple.split(SystemParameters.getString(conf, "DIP_GLOBAL_SPLIT_DELIMITER"));
            List<String> result= new ArrayList<String>();
            result.addAll(Arrays.asList(columnValues));
            return result;
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
            tupleString=tupleString.trim();
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


    /*
     * Method for creating joinParams (R.A=S.B) out of hashIndexes from the emitters from above.
     * If hash of R are fields 3,7 and out of S are 2,4, the final result is (3, 2, 7, 4)
     */
    public static List<Integer> combineHashIndexes(StormEmitter firstEmitter, StormEmitter secondEmitter) {
        List<Integer> hash1 = firstEmitter.getHashIndexes();
        List<Integer> hash2 = secondEmitter.getHashIndexes();

        if(hash1 == null || hash2 == null){
            // they are using hashExpressions only, no need for creating joinParams
            //   which is used for saving the space
            return null;
        }

        // check whether they are of eqaul size
        int hash1size = hash1.size();
        int hash2size = hash2.size();
        if(hash1size!=hash2size){
            throw new RuntimeException("Hash index size mismatch between " +
                    firstEmitter.getName() + " and " + secondEmitter.getName());
        }

        // take one HashIndex from each emitter at a time
        List<Integer> result = new ArrayList<Integer>();
        for(int i=0; i<2*hash1size; i++){
            if(i % 2 == 0){
                result.add(hash1.get(i/2));
            }else{
                result.add(hash2.get(i/2));
            }
        }
        return result;
    }

    public static List<String> createOutputTuple(List<String> firstTuple, List<String> secondTuple, List<Integer> joinParams) {
        List<String> outputTuple = new ArrayList<String>();

        for (int j = 0; j < firstTuple.size(); j++){ // add all elements of the first relation (R)
            outputTuple.add(firstTuple.get(j));
        }
        for (int j = 0; j < secondTuple.size(); j++) { // now add those
            if((joinParams == null) || (!joinIndexExist(joinParams, j, 1))){ //if does not exits add the column!! (S)
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


    private static boolean joinIndexExist(List<Integer> joinParams, int joinIndex, int beginIndex){
        boolean exists=false;
	for (int i = beginIndex; i < joinParams.size(); i++){
            if(joinParams.get(i)==joinIndex){
                exists=true;
                break;
            }
            i++;
	}
	return exists;
    }

   	public static boolean joinIndexExist(int[] joinParams, int joinIndex, int beginIndex){
		boolean exists=false;
		for (int i = beginIndex; i < joinParams.length; i++) {
			if(joinParams[i]==joinIndex){
                            exists=true;
                            break;
                        }
			i++;
		}
		return exists;
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
                int[] emitterIDs = emitter.getEmitterIDs();
                for(int emitterID: emitterIDs){
                    currentBolt = currentBolt.fieldsGrouping(Integer.toString(emitterID), new Fields("Hash"));
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
                int[] emitterIDs = emitter.getEmitterIDs();
                for(int emitterID: emitterIDs){
                    currentBolt = currentBolt.customGrouping(Integer.toString(emitterID),
                            new BalancedStreamGrouping(map, fullHashList));
                }
            }
            return currentBolt;
        }

        //if this is false, we have a specific mechanism to ensure all the tuples are fully processed
        //  it is based on CustomStreamGrouping
        public static boolean isAckEveryTuple(Map map){
            return (SystemParameters.getBoolean(map, "DIP_ACK_EVERY_TUPLE"));
        }

        public static boolean isFinalAck(String tupleString, Map map){
            return (!isAckEveryTuple(map)) && tupleString.equals("LAST_ACK");
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
                //  Anyway, we don't try to use StormSrcJoin with NoACK optimization.
                int[] ids = emitter.getEmitterIDs();
                for(int id: ids){
                    result += tc.getComponentTasks(String.valueOf(id)).size();
                }
            }
            return result;
        }

        public static <T extends Comparable<T>> List<ValueExpression> listTypeErasure(List<ValueExpression<T>> input){
            List<ValueExpression> result = new ArrayList<ValueExpression>();
            for(ValueExpression ve: input){
                result.add(ve);
            }
            return result;
        }
		
}