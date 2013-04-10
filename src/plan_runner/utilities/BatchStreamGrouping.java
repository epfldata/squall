package plan_runner.utilities;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class BatchStreamGrouping implements CustomStreamGrouping{

    //the number of tasks on the level this stream grouping is sending to
    private int _numTargetTasks;
    private List<Integer> _targetTasks;
    private List<String> _fullHashList;
    
    private Map _map;
    
    /*
     * fullHashList is null if grouping is not balanced
     */
    public BatchStreamGrouping(Map map, List<String> fullHashList){
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
        String tupleBatch = (String) stormTuple.get(1);
        if(MyUtilities.isFinalAckManualBatching(tupleBatch, _map)){
            //send to everyone
            return _targetTasks;
        }

        int endIndex = tupleBatch.indexOf(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
        String aHash = tupleBatch.substring(0, endIndex);

        if(!isBalanced()){
            return Arrays.asList(_targetTasks.get(MyUtilities.chooseHashTargetIndex(aHash, _numTargetTasks)));
        }else{
            return Arrays.asList(_targetTasks.get(MyUtilities.chooseBalancedTargetIndex(aHash, _fullHashList, _numTargetTasks)));
        }
    }

    private boolean isBalanced(){
        return (_fullHashList != null);
    }
}