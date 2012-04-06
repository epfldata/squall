/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import conversion.IntegerConversion;
import expressions.ColumnReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import org.apache.log4j.Logger;
import stormComponents.StormComponent;


public class LocalMergeResults {
        private static Logger LOG = Logger.getLogger(LocalMergeResults.class);
    
        //for writing the full final result in Local Mode
        private static int _collectedLastComponents = 0;
        private static int _receivedTuples = 0;
            //ReceivedTuples is the number of tuples the componentTask is reponsible for (!! not how many tuples are in storage!!)
        
        private static AggregateOperator _agg;
        private static Semaphore _semFullResult = new Semaphore(1, true);

        //The following 2 methods are crucial for collecting, printing and comparing the results in Local Mode
        //called on the component task level, when all Spouts fully propagated their tuples
        public static void localCollectFinalResult(AggregateOperator currentAgg, int hierarchyPosition, Map map, Logger log){
            if((!SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")) && hierarchyPosition == StormComponent.FINAL_COMPONENT){
                try {
                    //prepare it for printing at the end of the execution
                    _semFullResult.acquire();

                    _collectedLastComponents++;
                    _receivedTuples += currentAgg.tuplesProcessed();
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
            localPrint(_agg.printContent(), map);
            localCompare(_agg.getContent(), map);
        }

        private static void localPrint(String finalResult, Map map){
            StringBuilder sb = new StringBuilder();
            sb.append("\nThe full result for topology ");
            sb.append(MyUtilities.getFullTopologyName(map)).append(".");
            sb.append("\nCollected from ").append(_collectedLastComponents).append(" component tasks of the last component.");
            sb.append("\nAll the tasks of the last component in total received ").append(_receivedTuples).append(" tuples.");
            sb.append("\n").append(finalResult);
            LOG.info(sb.toString());
        }

        private static void localCompare(List<String> finalResult, Map map){
            
        }

        //result tuples are from second line, they look like:
        //  FURNITURE = 29074
        private static void addMoreResults(AggregateOperator currentAgg, Map map){
            if(_agg == null){
                _agg = new AggregateSumOperator(new IntegerConversion(), new ColumnReference(new IntegerConversion(), 1), map);
                _agg.setGroupByColumns(Arrays.asList(0));
            }
            _agg.addContent(currentAgg);
        }
}
