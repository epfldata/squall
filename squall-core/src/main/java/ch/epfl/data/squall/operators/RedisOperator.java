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

package ch.epfl.data.squall.operators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class RedisOperator extends OneToOneOperator {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(RedisOperator.class);
    private RedisConnection<String, String> redis;
    private int _numTuplesProcessed = 0;
    private int _sampleRate = 1000;
    private Map _map;

    public RedisOperator(Map map) {
		_map = map;
    }

    public void init_redis() {
        try {
            RedisClient client = new RedisClient(SystemParameters.getString(_map, "REDIS_SERVER"),
                    SystemParameters.getInt(_map, "REDIS_PORT"));
            redis = client.connect();
        }catch (RuntimeException err) {
            LOG.error(err);
            throw new RuntimeException("Error in Redis Connection\n " + err);
        }
    }

    @Override
    public void accept(OperatorVisitor ov) {
		ov.visit(this);
    }

    public void finalizeProcessing() {
    	try {
	    	redis.close();
	    } catch (RuntimeException err) {
	    	LOG.error(err);
    		throw new RuntimeException("Error in Redis close Connection\n " + err);
	    }
    }

    @Override
    public List<String> getContent() {
		throw new RuntimeException(
			"getContent for RedisOperator should never be invoked!");
    }

    @Override
    public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
		return false;
    }

    @Override
    public String printContent() {
		throw new RuntimeException(
			"printContent for RedisOperator should never be invoked!");
    }

    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
		_numTuplesProcessed++;
		String str = MyUtilities.tupleToString(tuple, _map);        
        
        if (redis == null)
            init_redis();

        if (_numTuplesProcessed % _sampleRate == 1) {
            redis.publish(SystemParameters.getString(_map, "REDIS_KEY"), str);
        }

		return tuple;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("RedisOperator ");
	return sb.toString();
    }
}
