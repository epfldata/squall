/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.utilities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import plan_runner.main.Main;
import plan_runner.operators.AggregateOperator;
import org.apache.log4j.Logger;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.storage.AggregationStorage;
import plan_runner.stormComponents.StormComponent;

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
                LOG.info("\nCannot validate the result, " + getResultFilePath(map) + " doesn't exist.");
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
                QueryPlan currentPlan = Main.queryPlan;
                _computedAgg = currentPlan.getOverallAggregation();
                _fileAgg = (AggregateOperator) DeepCopy.copy(_computedAgg);
                fillAggFromResultFile(map);
            }
            ((AggregationStorage)_computedAgg.getStorage()).addContent((AggregationStorage)(currentAgg.getStorage()));
        }

        private static void fillAggFromResultFile(Map map){
            String path = getResultFilePath(map);
            List<String> lines = MyUtilities.getLinesFromFile(path);
            if(lines!=null){
                for(String line: lines){
                    //List<String> tuple = Arrays.asList(line.split("\\s+=\\s+"));
                    //we want to catch exactly one space between and after =.
                    //  tuple might consist of spaces as well
                    List<String> tuple = Arrays.asList(line.split(" = "));
                    _fileAgg.process(tuple);
                }
            }else{
                //problem with finding the result file
                _fileAgg = null;
            }
        }

        private static String getResultFilePath(Map map){
            String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
            return System.getProperty("user.dir") + "/" + SystemParameters.RESULT_DIR + "/" + queryName + ".result";
        }
}
