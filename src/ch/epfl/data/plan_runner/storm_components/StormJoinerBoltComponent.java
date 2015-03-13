package ch.epfl.data.plan_runner.storm_components;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;

public abstract class StormJoinerBoltComponent extends StormBoltComponent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String _firstEmitterIndex, _secondEmitterIndex;
	protected ChainOperator _operatorChain;
	protected Predicate _joinPredicate;
	protected long _numSentTuples = 0;
	
	// join condition
	protected List<Index> _firstRelationIndexes, _secondRelationIndexes;
	protected List<Integer> _operatorForIndexes;
	protected List<Object> _typeOfValueIndexed;
	protected boolean _existIndexes = false;
	
	// for batch sending
	protected Semaphore _semAgg = new Semaphore(1, true);
	protected boolean _firstTime = true;
	protected PeriodicAggBatchSend _periodicAggBatch;
	protected long _aggBatchOutputMillis;

	// for printing statistics for creating graphs
	protected Calendar _cal = Calendar.getInstance();
	protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
	protected DateFormat _convDateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
	protected StatisticsUtilities _statsUtils;
	

	public StormJoinerBoltComponent(ComponentProperties cp,
			List<String> allCompNames, int hierarchyPosition,
			boolean isPartitioner, Map conf) {
		super(cp, allCompNames, hierarchyPosition, isPartitioner, conf);
	}
	
	public StormJoinerBoltComponent(ComponentProperties cp, List<String> allCompNames,
			int hierarchyPosition, Map conf) {
		super(cp, allCompNames, hierarchyPosition, conf);
		
		
	}
	
	
  

}