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

package ch.epfl.data.squall.storm_components;

import java.util.List;

import backtype.storm.tuple.Tuple;

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
