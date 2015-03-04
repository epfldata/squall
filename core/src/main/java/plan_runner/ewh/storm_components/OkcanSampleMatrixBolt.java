package plan_runner.ewh.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.conversion.NumericConversion;
import plan_runner.ewh.algorithms.OkcanCandidateInputAlgorithm;
import plan_runner.ewh.algorithms.OkcanCandidateOutputAlgorithm;
import plan_runner.ewh.algorithms.TilingAlgorithm;
import plan_runner.ewh.algorithms.optimality.WeightFunction;
import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.KeyRegion;
import plan_runner.ewh.data_structures.ListAdapter;
import plan_runner.ewh.data_structures.ListJavaGeneric;
import plan_runner.ewh.data_structures.ListTIntAdapter;
import plan_runner.ewh.data_structures.Region;
import plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import plan_runner.ewh.main.PushStatisticCollector;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.DeepCopy;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class OkcanSampleMatrixBolt<JAT extends Number & Comparable<JAT>> extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(OkcanSampleMatrixBolt.class);
	
	private StormEmitter _firstEmitter, _secondEmitter;
	private final String _firstEmitterIndex, _secondEmitterIndex;
	private final String _componentName;
	private NumericConversion _wrapper;
	private ComparisonPredicate _comparison;
	private Map _conf;
	private OutputCollector _collector;
	
	private int _xNumOfBuckets, _yNumOfBuckets;
	private int _numRemainingParents;
	private int _numOfLastJoiners; // input to tiling algorithm
	
	//private ListAdapter<JAT> _xJoinKeys = new ListJavaGeneric<JAT>();
	//private ListAdapter<JAT> _yJoinKeys = new ListJavaGeneric<JAT>();
	//private ListAdapter<JAT> _xJoinKeys = new ListTIntAdapter();
	//private ListAdapter<JAT> _yJoinKeys = new ListTIntAdapter();
	
	//to avoid some boxing/unboxing, we could directly use TIntList
	private ListAdapter<JAT> _xJoinKeys;
	private ListAdapter<JAT> _yJoinKeys;
	
	public OkcanSampleMatrixBolt(StormEmitter firstEmitter, StormEmitter secondEmitter, String componentName, int numOfLastJoiners,
			NumericConversion<JAT> wrapper, ComparisonPredicate comparison, int firstNumOfBuckets, int secondNumOfBuckets, 
			List<String> allCompNames, TopologyBuilder builder, TopologyKiller killer, Config conf) {
		
		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;
		_firstEmitterIndex = String.valueOf(allCompNames.indexOf(firstEmitter.getName()));
		_secondEmitterIndex = String.valueOf(allCompNames.indexOf(secondEmitter.getName()));
		_componentName = componentName;
		
		_numOfLastJoiners = numOfLastJoiners;
		_conf = conf;
		_comparison = comparison;
		_wrapper = wrapper;
		
		_xNumOfBuckets = firstNumOfBuckets;
		_yNumOfBuckets = secondNumOfBuckets;
		
		_xJoinKeys = MyUtilities.createListAdapter(conf);
		_yJoinKeys = MyUtilities.createListAdapter(conf);
		
		final int parallelism = 1;
		
		// connecting with previous level
		InputDeclarer currentBolt = builder.setBolt(componentName, this, parallelism);

		currentBolt = MyUtilities.attachEmitterToSingle(currentBolt, firstEmitter, secondEmitter);

		// connecting with Killer
		//if (getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf)))
		killer.registerComponent(this, componentName, parallelism);
	}

	@Override
	public void execute(Tuple stormTupleRcv) {
		final String inputComponentIndex = stormTupleRcv
				.getStringByField(StormComponent.COMP_INDEX); // getString(0);
		final List<String> tuple = (List<String>) stormTupleRcv
				.getValueByField(StormComponent.TUPLE); // getValue(1);

		if (processFinalAck(tuple, stormTupleRcv))
			return;

		processNonLastTuple(inputComponentIndex, tuple, stormTupleRcv, true);

		_collector.ack(stormTupleRcv);
	}

	// from IRichBolt
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return _conf;
	}

	private void processNonLastTuple(String inputComponentIndex, List<String> tuple,
			Tuple stormTupleRcv, boolean isLastInBatch) {

		if (_firstEmitterIndex.equals(inputComponentIndex)) {
			// R update
			String key = tuple.get(0);  // key is the only thing sent
			_xJoinKeys.add((JAT)_wrapper.fromString(key));
		} else if (_secondEmitterIndex.equals(inputComponentIndex)) {
			// S update
			String key = tuple.get(0);  // key is the only thing sent
			_yJoinKeys.add((JAT)_wrapper.fromString(key));
		} else
			throw new RuntimeException("InputComponentName " + inputComponentIndex
					+ " doesn't match neither " + _firstEmitterIndex + " nor "
					+ _secondEmitterIndex + ".");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//if (_hierarchyPosition == FINAL_COMPONENT) { // then its an intermediate
			// stage not the final
			// one
			//if (!MyUtilities.isAckEveryTuple(_conf))
				declarer.declareStream(SystemParameters.EOF_STREAM,
					new Fields(SystemParameters.EOF));
	}
	
	// BaseRichSpout
	@Override
	public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
		_collector = collector;
		_numRemainingParents = MyUtilities
				.getNumParentTasks(tc, Arrays.asList(_firstEmitter, _secondEmitter));
	}
		
	// if true, we should exit from method which called this method
	protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv) {
		if (MyUtilities.isFinalAck(tuple, _conf)) {
			_numRemainingParents--;
			if (_numRemainingParents == 0) {
				finalizeProcessing();
			}
			MyUtilities.processFinalAck(_numRemainingParents, StormComponent.FINAL_COMPONENT, _conf,
					stormTupleRcv, _collector);
			return true;
		}
		return false;
	}
	
	private void finalizeProcessing(){
		LOG.info("Before sorting keys");
		// sort keys
		_xJoinKeys.sort();
		_yJoinKeys.sort();
		LOG.info("After sorting keys");
		LOG.info("FirstKeys size is " + _xJoinKeys.size());
		LOG.info("SecondKeys size is " + _yJoinKeys.size());
		
		// create bucket boundaries
		// choose keys equi-distantly such that in total there are _numOfBuckets of them
		ListAdapter<JAT> xBoundaries = createBoundaries(_xJoinKeys, _xNumOfBuckets);
		ListAdapter<JAT> yBoundaries = createBoundaries(_yJoinKeys, _yNumOfBuckets);
		int xSize = xBoundaries.size();
		int ySize = yBoundaries.size();
		
		LOG.info("FirstBoundaries size is " + xSize);
		LOG.info("SecondBoundaries size is " + ySize);
		
		if(!SystemParameters.getBoolean(_conf, "DIP_DISTRIBUTED")){
			// this is just for local debugging
			LOG.info("FirstBoundaries are " + xBoundaries);
			LOG.info("SecondBoundaries are " + yBoundaries);
		}
		
		// create matrix but do not set output
		JoinMatrix<JAT> joinMatrix = new UJMPAdapterByteMatrix(xSize, ySize, _conf, _comparison, _wrapper);
		LOG.info("Capacity of coarsened joinMatrix in OkcanSampleMatrixBolt is " + joinMatrix.getCapacity());
		for(int i = 0; i < xSize; i++){
			joinMatrix.setJoinAttributeX(xBoundaries.get(i));
		}
		for(int i = 0; i < ySize; i++){
			joinMatrix.setJoinAttributeY(yBoundaries.get(i));
		}	

		LOG.info("joinMatrix in OkcanSampleMatrixBolt created with (" + xSize + ", " + ySize + ") number of buckets.");
		
		// create algos
		StringBuilder sb = new StringBuilder();		
		WeightFunction wf = new WeightFunction(1, 1);
		List<TilingAlgorithm> algorithms = new ArrayList<TilingAlgorithm>();
		algorithms.add(new OkcanCandidateInputAlgorithm(_numOfLastJoiners, wf, xSize, ySize, _conf));
		algorithms.add(new OkcanCandidateOutputAlgorithm(_numOfLastJoiners, wf, xSize, ySize, _conf));
			
		// run algos
		for(TilingAlgorithm algorithm: algorithms){
			try{
				LOG.info("Algorithm " + algorithm.toString() + " started.");
				
				long startTime = System.currentTimeMillis();
				sb = new StringBuilder();
				List<Region> regions = algorithm.partition(joinMatrix, sb);
				long endTime = System.currentTimeMillis();
				double elapsed = (endTime - startTime)/1000.0;
				// sbs are never printed out

				LOG.info("Algorithm " + algorithm.toString() + " completed.");
				
				// compute the joiner regions
				List<KeyRegion> keyRegions = PushStatisticCollector.generateKeyRegions(regions, joinMatrix, _wrapper);
				// we serialize and deserialize according to what is demanded, not what was the actual number of buckets
				// we don't use _xNumOfBuckets, because it can be changed from its original value specified in the config file
				String keyRegionFilename = MyUtilities.getKeyRegionFilename(_conf, algorithm.getShortName(), _numOfLastJoiners, SystemParameters.getInt(_conf, "FIRST_NUM_OF_BUCKETS"));
				LOG.info("keyRegionFilename = " + keyRegionFilename);
				
				// write KeyRegions
				//"Most impressive is that the entire process is JVM independent, 
				//    meaning an object can be serialized on one platform and deserialized on an entirely different platform."
				DeepCopy.serializeToFile(keyRegions, keyRegionFilename);
				keyRegions = (List<KeyRegion>) DeepCopy.deserializeFromFile(keyRegionFilename);
				LOG.info("Algorithm " + algorithm.toString() + " has " + KeyRegion.toString(keyRegions));

				// print regions
				LOG.info("Final regions are: " + Region.toString(regions, "Final"));
				LOG.info("\nElapsed algorithm time is " + elapsed + " seconds.\n");
				LOG.info("\n=========================================================================================\n");

				/*
				// read the existing KeyRegions (probably by BSP)
				String filenameBsp = MyUtilities.getKeyRegionFilename(_conf);
				keyRegions = (List<KeyRegion>) DeepCopy.deserializeFromFile(filenameBsp);
				LOG.info("PREVIOUS FILE " + KeyRegion.toString(keyRegions));
				*/
			}catch(Exception exc){
				LOG.info("EXCEPTION" + MyUtilities.getStackTrace(exc));
			}
		}
	}

	private ListAdapter<JAT> createBoundaries(ListAdapter<JAT> joinKeys, int numOfBuckets) {
		if(numOfBuckets > joinKeys.size()){
			numOfBuckets = joinKeys.size();
		}
		double distance = ((double)joinKeys.size()) / numOfBuckets;
		if(distance < 1){
			throw new RuntimeException("A bug: same element cannot be included more than once!");
		}
		
		ListAdapter<JAT> boundaries = MyUtilities.createListAdapter(_conf);
		for(int i = 0; i < numOfBuckets; i++){
			// We want to avoid high discrepancy between bucket sizes, which is a consequence of input sample size != 100 * n_s
			int index = (int) (i * distance + 0.5);
			boundaries.add((JAT) joinKeys.get(index));
		}
		
		return boundaries;
	}
}