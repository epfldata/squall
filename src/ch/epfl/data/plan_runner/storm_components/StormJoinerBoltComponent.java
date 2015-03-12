package ch.epfl.data.plan_runner.storm_components;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;

public abstract class StormJoinerBoltComponent extends StormBoltComponent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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