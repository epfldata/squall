package ch.epfl.data.squall.query_plans;

import java.util.ArrayList;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.StormOperator;

public class SimpleAggregationPlan extends QueryPlan {

	public SimpleAggregationPlan(String dataPath, String extension, Map conf) {
          super(dataPath, extension, conf);
	}

	@Override
	public Component createQueryPlan(String dataPath, String extension, Map conf) {
		// -------------------------------------------------------------------------------------
		Component sorted1 = new DataSourceComponent("SORTED-1", conf).setOutputPartKey(new int[] { 0 });
		Component sorted2 = new DataSourceComponent("SORTED-2", conf).setOutputPartKey(new int[] { 0 });
		Component sorted3 = new DataSourceComponent("SORTED-3", conf).setOutputPartKey(new int[] { 0 });
		
//		Component sorted1 = new DataSourceComponent("RANDOM-1", conf).setOutputPartKey(new int[] { 0 });
//		Component sorted2 = new DataSourceComponent("RANDOM-2", conf).setOutputPartKey(new int[] { 0 });
//		Component sorted = new DataSourceComponent("RANDOM-3", conf).setOutputPartKey(new int[] { 0 });
		
		ArrayList<Component> arr = new ArrayList<Component>();
		arr.add(sorted1);arr.add(sorted2);arr.add(sorted3);
//		Component sorted = new DataSourceComponent("RANDOM", conf).setOutputPartKey(new int[] { 0 });

		OperatorComponent op = new OperatorComponent(arr, "COUNT")
		.add(new AggregateCountOperator(conf).setGroupByColumns(0));
		
		// -------------------------------------------------------------------------------------

		return op;
		// -------------------------------------------------------------------------------------
	}
}
