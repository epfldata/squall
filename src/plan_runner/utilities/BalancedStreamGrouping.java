package plan_runner.utilities;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/*
 * If the list of all possible hash values isspecified,
 *   it uses uniform key (not number of tuples!) distribution
 * Otherwise, we encode fieldGrouping exactly the same as the Storm authors.
 * Because of NoACK possibility, have to be used everywhere in the code.
 */
public class BalancedStreamGrouping implements CustomStreamGrouping{

    //the number of tasks on the level this stream grouping is sending to
    private int _numTargetTasks;
    private List<Integer> _targetTasks;
    
    private List<String> _fullHashList;

    private Map _map;

    public BalancedStreamGrouping(Map map) {
        _map = map;
    }

    public BalancedStreamGrouping(Map map, List<String> fullHashList){
        _map = map;
        _fullHashList = fullHashList;
    }
    
    @Override
    public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        _numTargetTasks = targetTasks.size();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
        List<String> tuple = (List<String>) stormTuple.get(1);
        String tupleHash = (String) stormTuple.get(2);
        if(MyUtilities.isFinalAck(tuple, _map)){
            //send to everyone
            return _targetTasks;
        }
        if(!isBalanced()){
            return Arrays.asList(_targetTasks.get(fieldGrouping(tupleHash)));
        }else{
            return Arrays.asList(_targetTasks.get(balancedGrouping(tupleHash)));
        }
    }

    private boolean isBalanced(){
        return (_fullHashList != null);
    }

    private int fieldGrouping(String tupleHash){
        return Math.abs(tupleHash.hashCode()) % _numTargetTasks;
    }

    private int balancedGrouping(String tupleHash){
        return _fullHashList.indexOf(tupleHash) % _numTargetTasks;
    }

}