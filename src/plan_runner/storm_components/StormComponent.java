package plan_runner.storm_components;

import backtype.storm.tuple.Tuple;
import java.util.List;

public interface StormComponent {

    //Component which sends data further down
    public static final int INTERMEDIATE=1;
    //Last level component (can be part of Joiner), which does not propagate data anywhere
    public static final int FINAL_COMPONENT=2;
    
    // Tuple parts
	public static final String COMP_INDEX = "CompIndex";
	public static final String TUPLE = "Tuple";
	public static final String HASH = "Hash";
	public static final String TIMESTAMP = "Timestamp";    

    public String getID();
    public String getInfoID();

    public void printTupleLatency(long numSentTuples, long timestamp);
    public void printTuple(List<String> tuple);
    public void printContent();

    public void tupleSend(List<String> tuple, Tuple stormTupleRcv, long timestamp);
    //sending the current content of aggregation and then clearing it
    public void aggBatchSend();

}
