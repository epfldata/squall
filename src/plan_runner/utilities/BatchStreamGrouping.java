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

    private Map _map;

    public BatchStreamGrouping(Map map) {
        _map = map;
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

        return Arrays.asList(_targetTasks.get(MyUtilities.chooseTargetIndex(aHash, _numTargetTasks)));
    }
}