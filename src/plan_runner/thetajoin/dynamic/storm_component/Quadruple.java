package plan_runner.thetajoin.dynamic.storm_component;

import java.util.List;

import plan_runner.storage.TupleStorage;
import plan_runner.thetajoin.indexes.Index;

public class Quadruple {
	public TupleStorage affectedStorage,oppositeStorage;
	public List<Index> affectedIndexes, oppositeIndexes;
	public Quadruple(TupleStorage affectedStorage, TupleStorage oppositeStorage,
			List<Index> affectedIndexes, List<Index> oppositeIndexes) {
		this.affectedStorage = affectedStorage;
		this.oppositeStorage = oppositeStorage;
		this.affectedIndexes = affectedIndexes;
		this.oppositeIndexes = oppositeIndexes;
	}
	
	

	
}
