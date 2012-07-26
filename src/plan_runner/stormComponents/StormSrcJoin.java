package plan_runner.stormComponents;

import java.util.ArrayList;
import plan_runner.storage.BasicStore;
import plan_runner.stormComponents.synchronization.TopologyKiller;
import backtype.storm.Config;
import plan_runner.utilities.MyUtilities;
import backtype.storm.topology.TopologyBuilder;
import plan_runner.components.ComponentProperties;
import plan_runner.expressions.ValueExpression;
import java.io.Serializable;
import java.util.List;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.ProjectOperator;

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
                ComponentProperties cp,
                List<String> allCompNames,
                BasicStore<ArrayList<String>> firstPreAggStorage,
                BasicStore<ArrayList<String>> secondPreAggStorage,
                ProjectOperator firstPreAggProj,
                ProjectOperator secondPreAggProj,
                int hierarchyPosition,
                TopologyBuilder builder,
                TopologyKiller killer,
                Config conf){

            _componentName = cp.getName();
            _hashIndexes = cp.getHashIndexes();
            _hashExpressions = cp.getHashExpressions();
		
            //set the harmonizer
            _harmonizer= new StormSrcHarmonizer(_componentName,
                    firstEmitter,
                    secondEmitter,
                    builder,
                    killer,
                    conf);
		
            _firstStorage = new StormSrcStorage(firstEmitter,
                    secondEmitter,
                    cp,
                    allCompNames,
                    _harmonizer,
                    true,
                    firstPreAggStorage,
                    firstPreAggProj,
                    hierarchyPosition,
                    builder,
                    killer,
                    conf);
            _secondStorage = new StormSrcStorage(firstEmitter,
                    secondEmitter,
                    cp,
                    allCompNames,
                    _harmonizer,
                    false,
                    secondPreAggStorage,
                    secondPreAggProj,
                    hierarchyPosition,
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
        public String[] getEmitterIDs() {
            return new String[]{_firstStorage.getID(), _secondStorage.getID()};
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
