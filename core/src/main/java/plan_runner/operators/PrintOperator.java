package plan_runner.operators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import plan_runner.predicates.Predicate;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.visitors.OperatorVisitor;

public class PrintOperator implements Operator {
	private static final long serialVersionUID = 1L;

	private int _numTuplesProcessed = 0;
	private Map _map;
	private String _printPath;
	private PrintWriter _writer = null;
	
	// TODO 
	/*
	_writer.close
	} catch (IOException e) {
		    //exception handling left as an exercise for the reader
		}
		*/
	
	public PrintOperator(String filename, Map map) {
		_map = map;
		_printPath = SystemParameters.getString(map, "DIP_DATA_PATH") + "/" + filename;
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for PrintOperator should never be invoked!");
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
		throw new RuntimeException("printContent for PrintOperator should never be invoked!");
	}

	@Override
	public List<String> process(List<String> tuple) {
		if(_writer == null){
			try {
				//if(SystemParameters.getBoolean(_map, "DIP_DISTRIBUTED")){
				//	_printPath += "." + InetAddress.getLocalHost().getHostName();
				//}
				
				//initialize file to empty
				_writer = new PrintWriter(new BufferedWriter(new FileWriter(_printPath, false)));
				_writer.close();
				
				//open file for writing with append = true
				_writer = new PrintWriter(new BufferedWriter(new FileWriter(_printPath, true)));
			}catch (IOException e){
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

	public void finalizeProcessing() {
		if(_writer != null){
			_writer.close();
		}
	}
}