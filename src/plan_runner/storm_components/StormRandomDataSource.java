package plan_runner.storm_components;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.PeriodicBatchSend;
import plan_runner.utilities.SystemParameters;

/*
 * This class works only for Hyracks query.
 * A more generic approach is necessary to support randomization for other queries
 *    (an inline version of DBGEN).
 */
public class StormRandomDataSource extends BaseRichSpout implements StormEmitter, StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormRandomDataSource.class);

	private String _inputPath;
        private int _hierarchyPosition;
        
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

        private boolean _hasReachedEOF=false;
        private boolean _hasSentEOF = false; //have sent EOF to TopologyKiller (AckEachTuple mode)
	private boolean _hasSentLastAck = false; // AckLastTuple mode
        
	private long _pendingTuples=0;
	private String _ID;
        private String _componentIndex; //a unique index in a list of all the components
                            //used as a shorter name, to save some network traffic
                            //it's of type int, but we use String to save more space

        private Map _conf;
        private SpoutOutputCollector _collector;
        private ChainOperator _operatorChain;

        private int _numSentTuples=0;
        private boolean _printOut;

        private int _fileParts;


        //for batch sending
        private final Semaphore _semAgg = new Semaphore(1, true);
        private boolean _firstTime = true;
        private PeriodicBatchSend _periodicBatch;
        private long _batchOutputMillis;

        private int _generatedMax = 1500000;
        private int _customerTotal = 1500000;
        private int _ordersTotal = 15000000;

        private Random _randomGenerator = new Random();
        private int _tuplesProduced = -1;
        private int _customerProduced;
        private int _ordersProduced;

	public StormRandomDataSource(ComponentProperties cp,
                        List<String> allCompNames,
                        String inputPath,
                        int hierarchyPosition,
                        int parallelism,
                        TopologyBuilder	builder,
                        TopologyKiller killer,
                        Config conf) {
                _conf = conf;
                _operatorChain = cp.getChainOperator();
                _hierarchyPosition = hierarchyPosition;
		_ID=cp.getName();
		_componentIndex = String.valueOf(allCompNames.indexOf(cp.getName()));
                _inputPath=inputPath;
                _batchOutputMillis = cp.getBatchOutputMillis();

                _hashIndexes=cp.getHashIndexes();
                _hashExpressions = cp.getHashExpressions();
		
                _printOut = cp.getPrintOut();

                _fileParts = parallelism;

		if( _hierarchyPosition == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf))){
			killer.registerComponent(this, parallelism);
		}

		builder.setSpout(_ID, this, parallelism);
                if(MyUtilities.isAckEveryTuple(conf)){
                    killer.registerComponent(this, parallelism);
                }

                _customerProduced = _customerTotal/_fileParts;
                _ordersProduced = _ordersTotal/_fileParts;
	}

        // from IRichSpout interface
        @Override
	public void nextTuple() {
			if(_firstTime && MyUtilities.isBatchOutputMode(_batchOutputMillis)){
				_periodicBatch = new PeriodicBatchSend(_batchOutputMillis, this);
				_firstTime = false;
			}

			String line = readLine();
			if(line==null) {
                                if(!_hasReachedEOF){
                                    _hasReachedEOF=true;
                                    //we reached EOF, first time this happens we invoke the method:
                                    eofFinalization();
                                }
                                sendEOF();
                                //sleep since we are not goint to do useful work,
                                //  but still are looping in nextTuple method
				Utils.sleep(SystemParameters.EOF_TIMEOUT_MILLIS);
				return;
			}

			List<String> tuple = MyUtilities.fileLineToTuple(line, _conf);
			applyOperatorsAndSend(tuple);

	}

        protected void applyOperatorsAndSend(List<String> tuple){
            // do selection and projection
                if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
                    try {
                        _semAgg.acquire();
                    } catch (InterruptedException ex) {}
                }
		tuple = _operatorChain.process(tuple);
                if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
                    _semAgg.release();
                }

                if(tuple==null){
                    return;
                }

                _numSentTuples++;
                _pendingTuples++;
                printTuple(tuple);

                if(MyUtilities.isSending(_hierarchyPosition, _batchOutputMillis)){
                    tupleSend(tuple, null);
                }

        }

         /*
         * whatever is inside this method is done only once
         */
	private void eofFinalization(){
		printContent();

		if(!MyUtilities.isAckEveryTuple(_conf)){
			if(_hierarchyPosition == FINAL_COMPONENT){
                            if(!_hasSentEOF){
                                _hasSentEOF=true; // to ensure we will not send multiple EOF per single spout
                                _collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                            }
			} else {
                            if(!_hasSentLastAck){
                                _hasSentLastAck = true;
                                List<String> lastTuple = new ArrayList<String>(Arrays.asList(SystemParameters.LAST_ACK));
				_collector.emit(new Values("N/A", lastTuple, "N/A"));
                            }
                        }
		}
	}

        /*
         * sending EOF in AckEveryTuple mode when we send at least one tuple to the next component
         */
        private void sendEOF(){
            if (MyUtilities.isAckEveryTuple(_conf)){
                if (_pendingTuples == 0) {
                    if(!_hasSentEOF){
                        _hasSentEOF = true;
                        _collector.emit(SystemParameters.EOF_STREAM, new Values(SystemParameters.EOF));
                    }
                }
            }
        }

        @Override
        public void tupleSend(List<String> tuple, Tuple stormTupleRcv) {
            Values stormTupleSnd = MyUtilities.createTupleValues(tuple, _componentIndex,
                        _hashIndexes, _hashExpressions, _conf);
            MyUtilities.sendTuple(stormTupleSnd, _collector, _conf);
        }

        @Override
        public void batchSend(){
            if(MyUtilities.isBatchOutputMode(_batchOutputMillis)){
                if (_operatorChain != null){
                    Operator lastOperator = _operatorChain.getLastOperator();
                    if(lastOperator instanceof AggregateOperator){
                        try {
                            _semAgg.acquire();
                        } catch (InterruptedException ex) {}

                        //sending
                        AggregateOperator agg = (AggregateOperator) lastOperator;
                        List<String> tuples = agg.getContent();
                        for(String tuple: tuples){
                            tupleSend(MyUtilities.stringToTuple(tuple, _conf), null);
                        }

                        //clearing
                        agg.clearStorage();

                        _semAgg.release();
                    }
                }
            }
        }

	@Override
	public void open(Map map, TopologyContext tc, SpoutOutputCollector collector){
		_collector = collector;
	}

        //ack method on spout is called only if in AckEveryTuple mode (ACKERS > 0)
	@Override
	public void ack(Object msgId) {
            _pendingTuples--;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void fail(Object msgId) {
                throw new RuntimeException("Failing tuple in " + _ID);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
                if(MyUtilities.isAckEveryTuple(_conf) || _hierarchyPosition == FINAL_COMPONENT){
                    declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(SystemParameters.EOF));
                }
		declarer.declareStream(SystemParameters.DATA_STREAM, new Fields("CompIndex", "Tuple", "Hash"));
	}

        @Override
        public void printTuple(List<String> tuple){
            if(_printOut){
                if((_operatorChain == null) || !_operatorChain.isBlocking()){
                    StringBuilder sb = new StringBuilder();
                    sb.append("\nComponent ").append(_ID);
                    sb.append("\nReceived tuples: ").append(_numSentTuples);
                    sb.append(" Tuple: ").append(MyUtilities.tupleToString(tuple, _conf));
                    LOG.info(sb.toString());
                }
            }
        }

        @Override
        public void printContent() {
                if(_printOut){
                    if((_operatorChain != null) && _operatorChain.isBlocking()){
                        Operator lastOperator = _operatorChain.getLastOperator();
                        if (lastOperator instanceof AggregateOperator){
                            MyUtilities.printBlockingResult(_ID,
                                                        (AggregateOperator) lastOperator,
                                                        _hierarchyPosition,
                                                        _conf,
                                                        LOG);
                        }else{
                            MyUtilities.printBlockingResult(_ID,
                                                        lastOperator.getNumTuplesProcessed(),
                                                        lastOperator.printContent(),
                                                        _hierarchyPosition,
                                                        _conf,
                                                        LOG);
                        }
                    }
                }
        }

        // from StormComponent interface
        @Override
	public String getID() {
		return _ID;
	}

        // from StormEmitter interface
        @Override
        public String[] getEmitterIDs() {
            return new String[]{_ID};
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return _ID;
	}

        @Override
        public String getInfoID() {
            StringBuilder sb = new StringBuilder();
            sb.append("Table ").append(_ID).append(" has ID: ").append(_ID);
            return sb.toString();
        }

        // Helper methods
	public long getPendingTuples() {
		return _pendingTuples;
	}

//        private String readLine(){
//            String text=null;
//            try {
//                text=_reader.readLine();
//            } catch (IOException e) {
//                String errMessage = MyUtilities.getStackTrace(e);
//		LOG.info(errMessage);
//            }
//            return text;
//        }
        private String readLine(){
            _tuplesProduced++;
            String res = null;
            if(_ID.equalsIgnoreCase("Customer")){
                res = (_randomGenerator.nextInt(_generatedMax)) +"|Pera|palace|1|011|sdfa sdwe|FURNITURE|bla" ;
                if(_tuplesProduced == _customerProduced){
                    return null;
                }else{
                    return res;
                }
            }else {
                res = (_randomGenerator.nextInt(_generatedMax)) +"|1111|F|1022.34|1995-11-11|1-URGENT|mika|1-URGENT|no comment";
                if(_tuplesProduced == _ordersProduced){
                    return null;
                }else{
                    return res;
                }
            }
        }
}
