/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.window_semantics;

import java.util.List;

import org.apache.storm.tuple.Tuple;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class WindowSemanticsManager {

    public static boolean _IS_WINDOW_SEMANTICS = false;
    public static long _GC_PERIODIC_TICK = -1;
    public static final long INITIAL_TUMBLING_TIMESTAMP = System
	    .currentTimeMillis();

    public static String AddTimeStampToStoredDataIfWindowSemantics(
	    StormBoltComponent sbc, String inputTupleString, Tuple stormTupleRcv) {
	String result = inputTupleString;
	if (MyUtilities.isStoreTimestamp(sbc.getConf(),
		sbc.getHierarchyPosition())) {
	    final long incomingTimestamp = stormTupleRcv
		    .getLongByField(StormComponent.TIMESTAMP);
	    result = incomingTimestamp
		    + SystemParameters.STORE_TIMESTAMP_DELIMITER
		    + inputTupleString;
	}
	return result;
    }

    public static boolean evictStateIfSlidingWindowSemantics(
	    StormBoltComponent sbc, Tuple stormTupleRcv) {
	if ((_GC_PERIODIC_TICK > 0 | MyUtilities.isWindowTimestampMode(sbc
		.getConf())) & MyUtilities.isTickTuple(stormTupleRcv)) {
	    sbc.purgeStaleStateFromWindow();
	    sbc.getCollector().ack(stormTupleRcv);
	    return true;
	}
	return false;
    }

    /*
     * Populates the stringbuilder in accordance to the tuple format
     * (timestamps/or with out) and returns a response. -1 if wrong result, 0 if
     * not timestamp, or an actual (min/MAX) timestamp.
     */
    public static long joinPreProcessingIfSlidingWindowSemantics(
	    StormBoltComponent sbc, StringBuilder oppositeTupleString,
	    Tuple stormTuple) {
	// TODO
	long lineageTimestamp = 0;
	final long receivedTupleTimestamp;
	long storedTimestamp = 0;
	if (WindowSemanticsManager._IS_WINDOW_SEMANTICS
		| MyUtilities.isCustomTimestampMode(sbc.getConf()))
	    lineageTimestamp = stormTuple
		    .getLongByField(StormComponent.TIMESTAMP);
	receivedTupleTimestamp = lineageTimestamp;
	if (MyUtilities.isStoreTimestamp(sbc.getConf(),
		sbc.getHierarchyPosition())) {
	    // timestamp has to be removed
	    final String parts[] = oppositeTupleString.toString().split("\\@");
	    if (parts.length < 2)
		System.out.println("UNEXPECTED TIMESTAMP SIZES: "
			+ parts.length);
	    storedTimestamp = Long.valueOf(new String(parts[0]));
	    oppositeTupleString.setLength(0);
	    oppositeTupleString.append(parts[1]);
	    // now we set the maximum TS to the tuple
	    if (storedTimestamp > lineageTimestamp)
		lineageTimestamp = storedTimestamp;
	}
	// TODO

	// Sliding Window Overrides sliding windows TumblingWindow semantics
	// Check join condition for Window Joins (if not within boundaries,
	// skip)
	if (sbc._windowSize > 0) {
	    if (Math.abs(receivedTupleTimestamp - storedTimestamp) > sbc._windowSize)
		return -1;
	} else if (sbc._tumblingWindowSize > 0) {

	    long tumblingWindowStart = ((receivedTupleTimestamp - INITIAL_TUMBLING_TIMESTAMP) / sbc._tumblingWindowSize)
		    * sbc._tumblingWindowSize;
	    long tumblingWindowEnd = tumblingWindowStart
		    + sbc._tumblingWindowSize;
	    long storedTimeStampNormalized = (storedTimestamp - INITIAL_TUMBLING_TIMESTAMP);

	    if (!(storedTimeStampNormalized >= tumblingWindowStart && storedTimeStampNormalized <= tumblingWindowEnd))
		return -1;
	}

	return lineageTimestamp;
    }

    public static boolean sendTupleIfSlidingWindowSemantics(
	    StormBoltComponent sbc, List<String> tuple, Tuple stormTupleRcv,
	    long lineageTimestamp) {
	if (WindowSemanticsManager._IS_WINDOW_SEMANTICS) {
	    sbc.tupleSend(tuple, stormTupleRcv, lineageTimestamp);
	    return true;
	}
	return false;
    }

    public static void updateLatestTimeStamp(StormBoltComponent sbc,
	    Tuple stormTupleRcv) {
	if (sbc._isLocalWindowSemantics)
	    sbc._latestTimeStamp = stormTupleRcv
		    .getLongByField(StormComponent.TIMESTAMP);
    }

}
