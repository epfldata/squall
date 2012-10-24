package plan_runner.utilities;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.AggregateAvgOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.storage.AggregationStorage;
import plan_runner.storm_components.StormComponent;

public class LocalMergeResults {
        private static Logger LOG = Logger.getLogger(LocalMergeResults.class);

        //for writing the full final result in Local Mode
        private static int _collectedLastComponents = 0;
        private static int _numTuplesProcessed = 0;
            //the number of tuples the componentTask is reponsible for (!! not how many tuples are in storage!!)
        
        private static AggregateOperator _computedAgg;
        private static AggregateOperator _fileAgg;
        private static Semaphore _semFullResult = new Semaphore(1, true);

        //The following 2 methods are crucial for collecting, printing and comparing the results in Local Mode
        //called on the component task level, when all Spouts fully propagated their tuples
        public static void localCollectFinalResult(AggregateOperator lastAgg, int hierarchyPosition, Map map, Logger log){
            if((!SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")) && hierarchyPosition == StormComponent.FINAL_COMPONENT){
                try {
                    //prepare it for printing at the end of the execution
                    _semFullResult.acquire();

                    _collectedLastComponents++;
                    _numTuplesProcessed += lastAgg.getNumTuplesProcessed();
                    addMoreResults(lastAgg, map);

                    _semFullResult.release();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("InterruptedException unexpectedly occured!");
                }
            }
        }

        //called just before killExecution
        //only for local mode, since they are executed in a single process, sharing all the classes
        //  we need it due to collectedLastComponents, and lines of result
        //in cluster mode, they can communicate only through conf file
        public static void localPrintAndCompare(Map map) {
            localPrint(_computedAgg.printContent(), map);
            localCompare(map);
        }

        private static void localPrint(String finalResult, Map map){
            StringBuilder sb = new StringBuilder();
            sb.append("\nThe full result for topology ");
            sb.append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME")).append(".");
            sb.append("\nCollected from ").append(_collectedLastComponents).append(" component tasks of the last component.");
            sb.append("\nAll the tasks of the last component in total received ").append(_numTuplesProcessed).append(" tuples.");
            sb.append("\n").append(finalResult);
            LOG.info(sb.toString());
        }

        private static void localCompare(Map map){
            if(_fileAgg == null){
                LOG.info("\nCannot validate the result, result file " + getResultFilePath(map) + " does not exist."
                        + "\n  Make sure you specified correct DIP_RESULT_ROOT and"
                        + "\n  created result file with correct name.");
                return;
            }
            if(_computedAgg.printContent().isEmpty() || _fileAgg.printContent().isEmpty()){
                throw new RuntimeException("Final aggregation state should not be empty!");
            }
            if(_computedAgg.getStorage().equals(_fileAgg.getStorage())){
                LOG.info("\nOK: Expected result achieved for " + SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
            }else{
                StringBuilder sb = new StringBuilder();
                sb.append("\nPROBLEM: Not expected result achieved for ").append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
                sb.append("\nCOMPUTED: \n").append(_computedAgg.printContent());
                sb.append("\nFROM THE RESULT FILE: \n").append(_fileAgg.printContent());
                LOG.info(sb.toString());
            }
        }

        private static void addMoreResults(AggregateOperator lastAgg, Map map){
            if(_computedAgg == null){
                //first task of the last component asked to be added
                //we create empty aggregations, which we later fill, one from tasks, other from a file
                _computedAgg = createOverallAgg(lastAgg, map);
                _fileAgg = (AggregateOperator) DeepCopy.copy(_computedAgg);
                fillAggFromResultFile(map);
            }
            ((AggregationStorage)_computedAgg.getStorage()).addContent((AggregationStorage)(lastAgg.getStorage()));
        }
        
        private static AggregateOperator createOverallAgg(AggregateOperator lastAgg, Map map){
            TypeConversion wrapper = lastAgg.getType();
            AggregateOperator overallAgg;
            
            ColumnReference cr;
            if(lastAgg.hasGroupBy()){
                cr = new ColumnReference(wrapper, 1);
            }else{
                cr = new ColumnReference(wrapper, 0);
            }
            
            if(lastAgg instanceof AggregateAvgOperator){
                overallAgg = new AggregateAvgOperator(cr, map);
            }else{
                overallAgg = new AggregateSumOperator(cr, map);
            }
            
            if(lastAgg.hasGroupBy()){
                overallAgg.setGroupByColumns(Arrays.asList(0));
            }
            
            return overallAgg;
        }

        private static void fillAggFromResultFile(Map map){
            try {
                String path = getResultFilePath(map);
                List<String> lines = MyUtilities.readFileLinesSkipEmpty(path);

                for(String line: lines){
                    //List<String> tuple = Arrays.asList(line.split("\\s+=\\s+"));
                    //we want to catch exactly one space between and after =.
                    //  tuple might consist of spaces as well
                    List<String> tuple = Arrays.asList(line.split(" = "));
                    _fileAgg.process(tuple);
                }
            } catch (IOException ex) {
                //problem with finding the result file
                _fileAgg = null;
            }
        }

        private static String getResultFilePath(Map map){
            String rootDir = getResultDir(map);
            String schemaName = getSchemaName(map);
            String dataSize = getDataSizeInfo(map);
            String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
            return  rootDir + "/" + schemaName + "/" + dataSize + "/" + queryName + ".result";
        }
        
        // this has to be a separate method, because we don't want Exception if DIP_RESULT_ROOT is not set
        private static String getResultDir(Map map) {
            String resultRoot = "";
            if (SystemParameters.isExisting(map, "DIP_RESULT_ROOT")){
                resultRoot = SystemParameters.getString(map, "DIP_RESULT_ROOT");
            }
            return resultRoot;
        }
        
        /*
         * from "../test/data/tpch/0.01G" as dataPath,
         *   return tpch
         */
        private static String getSchemaName(Map map) {
            String path = SystemParameters.getString(map, "DIP_DATA_PATH");
            return MyUtilities.getPartFromEnd(path, 1);
        }        

        //getting size information - from path "../test/data/tpch/0.01G", 
        //  it extracts dataSize = 0.01G        
        //For Squall (not in Squall Plan Runner) there is DIP_DB_SIZE, 
        //  but this method has to be used for PlanRunner as well.
        private static String getDataSizeInfo(Map map) {
            String path = SystemParameters.getString(map, "DIP_DATA_PATH");
            return MyUtilities.getPartFromEnd(path, 0);
        }
}
