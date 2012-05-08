package stormComponents;

import storage.SquallStorage;
import stormComponents.synchronization.TopologyKiller;
import backtype.storm.Config;
import utilities.MyUtilities;
import backtype.storm.topology.TopologyBuilder;
import expressions.ValueExpression;
import java.io.Serializable;
import java.util.List;
import operators.AggregateOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;

import org.apache.log4j.Logger;

public class StormSrcJoin implements StormJoin, Serializable{
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(StormSrcJoin.class);
      
	private StormSrcHarmonizer _harmonizer=null;
	private StormSrcStorage _firstStorage=null;
	private StormSrcStorage _secondStorage=null;

        private String _componentName;
        private List<Integer> _hashIndexes;
        private List<ValueExpression> _hashExpressions;

	public StormSrcJoin(StormEmitter firstEmitter,
                StormEmitter secondEmitter,
                String componentName,
                SelectionOperator selection,
                ProjectionOperator projection,
                AggregateOperator aggregation,
                SquallStorage firstPreAggStorage,
                SquallStorage secondPreAggStorage,
                ProjectionOperator firstPreAggProj,
                ProjectionOperator secondPreAggProj,
                List<Integer> hashIndexes,
                List<ValueExpression> hashExpressions,
                int hierarchyPosition,
                boolean printOut,
                long batchOutputMillis,
                TopologyBuilder builder,
                TopologyKiller killer,
                Config conf){

            _componentName = componentName;
            _hashIndexes = hashIndexes;
            _hashExpressions = hashExpressions;
		
            //set the harmonizer
            _harmonizer= new StormSrcHarmonizer(componentName,
                    firstEmitter,
                    secondEmitter,
                    builder,
                    killer,
                    conf);
		
            List<Integer> joinParams = MyUtilities.combineHashIndexes(firstEmitter, secondEmitter);
            _firstStorage = new StormSrcStorage(componentName,
                    firstEmitter.getName(),
                    _harmonizer,
                    joinParams,
                    true,
                    selection,
                    projection,
                    aggregation,
                    firstPreAggStorage,
                    firstPreAggProj,
                    hashIndexes,
                    hashExpressions,
                    hierarchyPosition,
                    printOut,
                    batchOutputMillis,
                    builder,
                    killer,
                    conf);
            _secondStorage = new StormSrcStorage(componentName,
                    secondEmitter.getName(),
                    _harmonizer,
                    joinParams,
                    false,
                    selection,
                    projection,
                    aggregation,
                    secondPreAggStorage,
                    secondPreAggProj,
                    hashIndexes,
                    hashExpressions,
                    hierarchyPosition,
                    printOut,
                    batchOutputMillis,
                    builder,
                    killer,
                    conf);

            if(!MyUtilities.isAckEveryTuple(conf)){
                throw new RuntimeException("You must use StormDstJoin if you want to ACK only at the end!");
            }
	}

        // from StormEmitter interface
        @Override
        public String getName() {
            return _componentName;
        }

        @Override
        public int[] getEmitterIDs() {
            return new int[]{_firstStorage.getID(), _secondStorage.getID()};
        }

        @Override
        public List<Integer> getHashIndexes() {
            return _hashIndexes;
        }

        @Override
        public List<ValueExpression> getHashExpressions() {
            return _hashExpressions;
        }

        @Override
        public String getInfoID() {
            StringBuilder sb = new StringBuilder();
            sb.append(_harmonizer.getInfoID()).append("\n");
            sb.append(_firstStorage.getInfoID()).append("\n");
            sb.append(_secondStorage.getInfoID()).append("\n");
            return sb.toString();
        }
}
