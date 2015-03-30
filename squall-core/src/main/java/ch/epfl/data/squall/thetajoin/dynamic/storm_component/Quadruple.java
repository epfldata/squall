package ch.epfl.data.squall.thetajoin.dynamic.storm_component;

import java.util.List;

import ch.epfl.data.squall.storage.TupleStorage;
import ch.epfl.data.squall.thetajoin.indexes.Index;

public class Quadruple {
	public TupleStorage affectedStorage, oppositeStorage;
	public List<Index> affectedIndexes, oppositeIndexes;

	public Quadruple(TupleStorage affectedStorage,
			TupleStorage oppositeStorage, List<Index> affectedIndexes,
			List<Index> oppositeIndexes) {
		this.affectedStorage = affectedStorage;
		this.oppositeStorage = oppositeStorage;
		this.affectedIndexes = affectedIndexes;
		this.oppositeIndexes = oppositeIndexes;
	}

}
