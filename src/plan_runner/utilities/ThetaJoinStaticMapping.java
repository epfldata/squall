package plan_runner.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import plan_runner.thetajoin.matrixMapping.MatrixAssignment;
import plan_runner.thetajoin.matrixMapping.MatrixAssignment.Dimension;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;

public class ThetaJoinStaticMapping implements CustomStreamGrouping{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinStaticMapping.class);

	private MatrixAssignment _assignment;
	private String _firstIndex, _secondIndex;
	private int _numTasks;
	private Map _map;

	public ThetaJoinStaticMapping(String first,String second,MatrixAssignment assignment, Map map) {
		_assignment=assignment;
		_firstIndex=first;
		_secondIndex=second;
		_map=map;
	}

	@Override
	public void prepare(Fields outFields, int numTasks) {
		//LOG.info("Number of tasks is : "+numTasks);
		_numTasks = numTasks;
		
	}

	@Override
	public List<Integer> taskIndices(List<Object> values) {
		
		//////////////////////
            List<String> tuple = (List<String>) values.get(1);
            if(MyUtilities.isFinalAck(tuple, _map)){
                List<Integer> result = new ArrayList<Integer>();
                for(int i=0; i< _numTasks; i++){
                    result.add(i);
                }
                return result;
            }
            
		//////////////////
		List<Integer> tasks=null;
		String tableName= (String)values.get(0);
		if(tableName.equals(_firstIndex))
			tasks= _assignment.getRegionIDs(Dimension.ROW);
		else if(tableName.equals(_secondIndex))
			tasks= _assignment.getRegionIDs(Dimension.COLUMN);
		else{
			LOG.info("First Name: "+_firstIndex);
			LOG.info("Second Name: "+_secondIndex);
			LOG.info("Table Name: "+tableName);
			LOG.info("WRONG ASSIGNMENT");
		}
		//LOG.info(tasks);
		return tasks;
	}

}
