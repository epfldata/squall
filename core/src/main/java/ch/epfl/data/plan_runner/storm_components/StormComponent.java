package ch.epfl.data.plan_runner.storm_components;

import java.util.List;

public interface StormComponent {

	// Component which sends data further down
	public static final int INTERMEDIATE = 1;
	// Last level component (can be part of Joiner), which does not propagate
	// data anywhere
	public static final int FINAL_COMPONENT = 2;
	public static final int NEXT_TO_LAST_COMPONENT = 3;
	public static final int NEXT_TO_DUMMY = 4;

	// Tuple parts
	public static final String COMP_INDEX = "CompIndex";
	public static final String TUPLE = "Tuple";
	public static final String HASH = "Hash";
	public static final String TIMESTAMP = "Timestamp";
	public static final String EPOCH = "Epoch";
	public static final String MESSAGE = "Message";
	public static final String DIM = "DIM";
	public static final String RESH_SIGNAL = "ReshufflerSignal";
	public static final String MAPPING = "Mapping";

	// sending the current content of aggregation and then clearing it
	public void aggBatchSend();

	public String getID();

	public String getInfoID();

	public void printContent();

	public void printTuple(List<String> tuple);

	public void printTupleLatency(long numSentTuples, long timestamp);

	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp);

}
