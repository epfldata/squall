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

import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class PrintOperator implements Operator {
    private static final long serialVersionUID = 1L;

    private int _numTuplesProcessed = 0;
    private Map _map;
    private String _printPath;
    private PrintWriter _writer = null;

    // TODO
    /*
     * _writer.close } catch (IOException e) { //exception handling left as an
     * exercise for the reader }
     */

    public PrintOperator(String filename, Map map) {
	_map = map;
	_printPath = SystemParameters.getString(map, "DIP_DATA_PATH") + "/"
		+ filename;
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    public void finalizeProcessing() {
	if (_writer != null) {
	    _writer.close();
	}
    }

    @Override
    public List<String> getContent() {
	throw new RuntimeException(
		"getContent for PrintOperator should never be invoked!");
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
		"printContent for PrintOperator should never be invoked!");
    }

    @Override
    public List<String> process(List<String> tuple, long lineageTimestamp) {
	if (_writer == null) {
	    try {
		// if(SystemParameters.getBoolean(_map, "DIP_DISTRIBUTED")){
		// _printPath += "." + InetAddress.getLocalHost().getHostName();
		// }

		// initialize file to empty
		_writer = new PrintWriter(new BufferedWriter(new FileWriter(
			_printPath, false)));
		_writer.close();

		// open file for writing with append = true
		_writer = new PrintWriter(new BufferedWriter(new FileWriter(
			_printPath, true)));
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}
	_numTuplesProcessed++;
	String str = MyUtilities.tupleToString(tuple, _map);
	_writer.println(str);
	return tuple;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("PrintOperator ");
	return sb.toString();
    }
}