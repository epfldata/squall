package utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import thetajoin.matrixMapping.MatrixAssignment;
import thetajoin.matrixMapping.MatrixAssignment.Dimension;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;

public class ThetaJoinStaticMapping implements CustomStreamGrouping{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ThetaJoinStaticMapping.class);

	private MatrixAssignment _assignment;
	private String _first, _second;
	private int _numTasks;
	private Map _map;

	public ThetaJoinStaticMapping(String first,String second,MatrixAssignment assignment, Map map) {
		_assignment=assignment;
		_first=first;
		_second=second;
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
		String tupleString = (String) values.get(1);
		if(MyUtilities.isFinalAck(tupleString, _map)){
            List<Integer> result = new ArrayList<Integer>();
            for(int i=0; i< _numTasks; i++){
                result.add(i);
            }
            return result;
        }
		//////////////////
		List<Integer> tasks=null;
		String tableName= (String)values.get(0);
		if(tableName.equals(_first))
			tasks= _assignment.getRegionIDs(Dimension.ROW);
		else if(tableName.equals(_second))
			tasks= _assignment.getRegionIDs(Dimension.COLUMN);
		else
			LOG.info("WRONG ASSIGNMENT");
		//LOG.info(tasks);
		return tasks;
	}

}
