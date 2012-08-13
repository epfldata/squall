package plan_runner.utilities;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import plan_runner.main.Main;
import plan_runner.operators.AggregateOperator;
import plan_runner.query_plans.QueryPlan;
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
        public static void localCollectFinalResult(AggregateOperator currentAgg, int hierarchyPosition, Map map, Logger log){
            if((!SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")) && hierarchyPosition == StormComponent.FINAL_COMPONENT){
                try {
                    //prepare it for printing at the end of the execution
                    _semFullResult.acquire();

                    _collectedLastComponents++;
                    _numTuplesProcessed += currentAgg.getNumTuplesProcessed();
                    addMoreResults(currentAgg, map);

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
            sb.append(MyUtilities.getFullTopologyName(map)).append(".");
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
            if(_computedAgg.getStorage().equals(_fileAgg.getStorage())){
                LOG.info("\nOK: Expected result achieved for " + MyUtilities.getFullTopologyName(map));
            }else{
                StringBuilder sb = new StringBuilder();
                sb.append("\nPROBLEM: Not expected result achieved for ").append(MyUtilities.getFullTopologyName(map));
                sb.append("\nCOMPUTED: \n").append(_computedAgg.printContent());
                sb.append("\nFROM THE RESULT FILE: \n").append(_fileAgg.printContent());
                LOG.info(sb.toString());
            }
        }

        private static void addMoreResults(AggregateOperator currentAgg, Map map){
            if(_computedAgg == null){
                //first task of the last component asked to be added
                //we create empty aggregations, which we later fill, one from tasks, other from a file
                //QueryPlan currentPlan = Main.queryPlan;
                QueryPlan currentPlan = Main.chooseQueryPlan(map);
                _computedAgg = currentPlan.getOverallAggregation();
                _fileAgg = (AggregateOperator) DeepCopy.copy(_computedAgg);
                fillAggFromResultFile(map);
            }
            ((AggregationStorage)_computedAgg.getStorage()).addContent((AggregationStorage)(currentAgg.getStorage()));
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
         * from "$PATH/tpch.txt", it returns "tpch"
         */
        private static String getSchemaName(Map map) {
            if(!SystemParameters.isExisting(map, "DIP_SCHEMA_PATH")){
                //DIP_SCHEMA_PATH does not exist when PlanRunner is directly invoked
                //  but we know that all of them are tpch
                return "tpch";
                
            }
            String schemaPath = SystemParameters.getString(map, "DIP_SCHEMA_PATH");
            
            int pos = schemaPath.lastIndexOf("/");
            String schemaFilename = schemaPath.substring(pos + 1, schemaPath.length());
            
            String parts[] = schemaFilename.split("\\.");
            return parts[0];
        }        

        //getting size information - from path "../test/data/tpch/0.01G", 
        //  it extracts dataSize = 0.01G        
        //For Squall (not in Squall Plan Runner) there is DIP_DB_SIZE, 
        //  but this method has to be used for PlanRunner as well.
        private static String getDataSizeInfo(Map map) {
            String path = SystemParameters.getString(map, "DIP_DATA_PATH");
            if(path.endsWith("/")){
                //removing last "/" character
                path = path.substring(0, path.length()-1);
            }
            int pos = path.lastIndexOf("/");
            return path.substring(pos + 1, path.length());
        }
}