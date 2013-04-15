package plan_runner.storm_components;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicAggBatchSend;
import plan_runner.utilities.SystemParameters;

/*
 * This class works only for Hyracks query.
 * A more generic approach is necessary to support randomization for other queries
 *    (an inline version of DBGEN).
 */
public class StormRandomDataSource extends StormDataSource {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormRandomDataSource.class);

    private int _generatedMax = 1500000;
    private int _customerTotal = 1500000;
    private int _ordersTotal = 15000000;

    private Random _randomGenerator = new Random();
    private int _tuplesProduced = -1;
    private int _customerProduced;
    private int _ordersProduced;
        
   public StormRandomDataSource(ComponentProperties cp,
                        List<String> allCompNames,
                        String inputPath,
                        int hierarchyPosition,
                        int parallelism,
                        TopologyBuilder	builder,
                        TopologyKiller killer,
                        Config conf) {
        super(cp, allCompNames, inputPath, hierarchyPosition, parallelism, builder, killer, conf);

        _customerProduced = _customerTotal/parallelism;
        _ordersProduced = _ordersTotal/parallelism;
	}

    @Override
    protected String readLine(){
    	_tuplesProduced++;
        String res = null;
        if(getID().equalsIgnoreCase("Customer")){
        	res = (_randomGenerator.nextInt(_generatedMax)) +"|Pera|palace|1|011|sdfa sdwe|FURNITURE|bla" ;
            if(_tuplesProduced == _customerProduced){
            	return null;
            }else{
            	return res;
            }
        }else {
        	res = (_randomGenerator.nextInt(_generatedMax)) +"|1111|F|1022.34|1995-11-11|1-URGENT|mika|1-URGENT|no comment";
            if(_tuplesProduced == _ordersProduced){
            	return null;
            }else{
            	return res;
            }
        }
   }
}
