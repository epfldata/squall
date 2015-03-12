package ch.epfl.data.plan_runner.storm_components;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
@Deprecated
public class StormSrcJoin implements StormJoin, Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormSrcJoin.class);

	private StormSrcHarmonizer _harmonizer = null;
	private StormSrcStorage _firstStorage = null;
	private StormSrcStorage _secondStorage = null;

	private final String _componentName;
	private final List<Integer> _hashIndexes;
	private final List<ValueExpression> _hashExpressions;

	public StormSrcJoin(StormEmitter firstEmitter, StormEmitter secondEmitter,
			ComponentProperties cp, List<String> allCompNames,
			BasicStore<ArrayList<String>> firstPreAggStorage,
			BasicStore<ArrayList<String>> secondPreAggStorage, ProjectOperator firstPreAggProj,
			ProjectOperator secondPreAggProj, int hierarchyPosition, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {

		_componentName = cp.getName();
		_hashIndexes = cp.getHashIndexes();
		_hashExpressions = cp.getHashExpressions();

		// set the harmonizer
		_harmonizer = new StormSrcHarmonizer(_componentName, firstEmitter, secondEmitter, builder,
				killer, conf);

		_firstStorage = new StormSrcStorage(firstEmitter, secondEmitter, cp, allCompNames,
				_harmonizer, true, firstPreAggStorage, firstPreAggProj, hierarchyPosition, builder,
				killer, conf);
		_secondStorage = new StormSrcStorage(firstEmitter, secondEmitter, cp, allCompNames,
				_harmonizer, false, secondPreAggStorage, secondPreAggProj, hierarchyPosition,
				builder, killer, conf);

		if (!MyUtilities.isAckEveryTuple(conf))
			throw new RuntimeException(
					"You must use StormDstJoin if you want to ACK only at the end!");
	}

	@Override
	public String[] getEmitterIDs() {
		return new String[] { _firstStorage.getID(), _secondStorage.getID() };
	}

	@Override
	public String getInfoID() {
		final StringBuilder sb = new StringBuilder();
		sb.append(_harmonizer.getInfoID()).append("\n");
		sb.append(_firstStorage.getInfoID()).append("\n");
		sb.append(_secondStorage.getInfoID()).append("\n");
		return sb.toString();
	}

	// from StormEmitter interface
	@Override
	public String getName() {
		return _componentName;
	}
}
