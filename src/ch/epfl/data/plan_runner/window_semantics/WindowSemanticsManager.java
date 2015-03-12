package ch.epfl.data.plan_runner.window_semantics;

import java.util.List;

import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class WindowSemanticsManager {
	
	
	public static boolean evictStateIfSlidingWindowSemantics(StormBoltComponent sbc, Tuple stormTupleRcv){
		if (MyUtilities.isWindowTimestampMode(sbc.getConf()) & MyUtilities.isTickTuple(stormTupleRcv)) { 
			sbc.purgeStaleStateFromWindow();
			sbc.getCollector().ack(stormTupleRcv);
			return true;
		}
		return false;
	}
	
	public static boolean sendTupleIfSlidingWindowSemantics(StormBoltComponent sbc, List<String> tuple,Tuple stormTupleRcv, long lineageTimestamp){
		if(MyUtilities.isWindowTimestampMode(sbc.getConf())){
			sbc.tupleSend(tuple, stormTupleRcv, lineageTimestamp);
			return true;
			}
		return false;
	}
	
	public static void updateLatestTimeStamp(StormBoltComponent sbc,Tuple stormTupleRcv){
		if (MyUtilities.isWindowTimestampMode(sbc.getConf()))
			sbc._latestTimeStamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
	}
	
	/*
	 * Populates the stringbuilder in accordance to the tuple format (timestamps/or with out) and returns a response.
	 *  -1 if wrong result, 0 if not timestamp, or an actual (min/MAX) timestamp. 
	 */
	public static long joinPreProcessingIfSlidingWindowSemantics(StormBoltComponent sbc, StringBuilder oppositeTupleString, Tuple stormTuple){
		//TODO
		long lineageTimestamp = 0;
		final long receivedTupleTimestamp;
		long storedTimestamp = 0;
		if (MyUtilities.isCustomTimestampMode(sbc.getConf()) || MyUtilities.isWindowTimestampMode(sbc.getConf()))
			lineageTimestamp = stormTuple.getLongByField(StormComponent.TIMESTAMP);
		receivedTupleTimestamp = lineageTimestamp;
		if (MyUtilities.isStoreTimestamp(sbc.getConf(), sbc.getHierarchyPosition())) {
			// timestamp has to be removed
			final String parts[] = oppositeTupleString.toString().split("\\@");
			if(parts.length<2) System.out.println("UNEXPECTED TIMESTAMP SIZES: "+parts.length);
			storedTimestamp = Long.valueOf(new String(parts[0]));
			oppositeTupleString.setLength(0); 
			oppositeTupleString.append(parts[1]);
			// now we set the maximum TS to the tuple
			if (storedTimestamp > lineageTimestamp)
				lineageTimestamp = storedTimestamp;
		}
		//TODO
		
		// Sliding Window Overrides sliding windows TumblingWindow semantics
		//Check join condition for Window Joins (if not within boundaries, skip)
		if (sbc._windowSize > 0) {
			if (Math.abs(receivedTupleTimestamp - storedTimestamp) > sbc._windowSize)
				return -1;
		}
		else if(sbc._tumblingWindowSize > 0){
			
			long tumblingWindowStart = (receivedTupleTimestamp - sbc.INITIAL_TUMBLING_TIMESTAMP)/sbc._tumblingWindowSize;
			long tumblingWindowEnd = tumblingWindowStart+sbc._tumblingWindowSize;   
			long storedTimeStampNormalized= storedTimestamp - sbc.INITIAL_TUMBLING_TIMESTAMP;
			
			if( !(storedTimeStampNormalized >= tumblingWindowStart &&  storedTimeStampNormalized <= tumblingWindowEnd))
				return -1; 
		}
		
		return lineageTimestamp;
	}
	
	public static String AddTimeStampToStoredDataIfWindowSemantics(StormBoltComponent sbc, String inputTupleString,Tuple stormTupleRcv){
		String result=inputTupleString;
		if (MyUtilities.isStoreTimestamp(sbc.getConf(), sbc.getHierarchyPosition())) {
			final long incomingTimestamp = stormTupleRcv.getLongByField(StormComponent.TIMESTAMP);
			result = incomingTimestamp + SystemParameters.STORE_TIMESTAMP_DELIMITER
					+ inputTupleString;
		}
		return result;
	}
	

}
