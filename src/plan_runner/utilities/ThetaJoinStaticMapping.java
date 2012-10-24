package plan_runner.utilities;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.thetajoin.matrix_mapping.MatrixAssignment;
import plan_runner.thetajoin.matrix_mapping.MatrixAssignment.Dimension;

public class ThetaJoinStaticMapping implements CustomStreamGrouping{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinStaticMapping.class);

	private MatrixAssignment _assignment;
	private String _firstIndex, _secondIndex;
        private List<Integer> _targetTasks;
	private Map _map;

	public ThetaJoinStaticMapping(String first, String second, MatrixAssignment assignment, Map map) {
		_assignment=assignment;
		_firstIndex=first;
		_secondIndex=second;
		_map=map;
	}

	@Override
	public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi, List<Integer> targetTasks){
		//LOG.info("Number of tasks is : "+numTasks);
                _targetTasks = targetTasks;
	}

	//@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		
		//////////////////////
            List<String> tuple = (List<String>) values.get(1);
            if(MyUtilities.isFinalAck(tuple, _map)){
                return _targetTasks;
            }
            
		//////////////////
		List<Integer> tasks=null;
		String tableName= (String)values.get(0);
		if(tableName.equals(_firstIndex))
			tasks= translateIdsToTasks(_assignment.getRegionIDs(Dimension.ROW));
		else if(tableName.equals(_secondIndex))
			tasks= translateIdsToTasks(_assignment.getRegionIDs(Dimension.COLUMN));
		else{
			LOG.info("First Name: "+_firstIndex);
			LOG.info("Second Name: "+_secondIndex);
			LOG.info("Table Name: "+tableName);
			LOG.info("WRONG ASSIGNMENT");
		}
		//LOG.info(tasks);
		return tasks;
	}

    private List<Integer> translateIdsToTasks(ArrayList<Integer> ids) {
        List<Integer> converted = new ArrayList<Integer>();
        for(int id: ids){
            converted.add(_targetTasks.get(id));
        }
        return converted;
    }

}