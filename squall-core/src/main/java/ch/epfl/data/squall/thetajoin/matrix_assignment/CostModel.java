package ch.epfl.data.squall.thetajoin.matrix_assignment;

import java.io.Serializable;
import java.util.Comparator;

/**
 * The cost model to compare two different partitioning results. In this case, we consider both computation cost and communication cost
 */
class CombineCost implements Comparator<Assignment>, Serializable {
	
	private CommCost c1 = new CommCost();
	private CompCost c2 = new CompCost();

	@Override
	public int compare(Assignment o1, Assignment o2) {
		if (c1.compare(o1, o2) >= 0 && c2.compare(o1, o2) >= 0) return 1;
		else return 0;
	}
	
}

/**
 * Computation cost: the number of joins each machine has to process
 */
class CompCost implements Comparator<Assignment>, Serializable {
	
	private double computationCost(long[] sizes, int[] rd) {
		double cost = 1;
		for (int i = 0; i < sizes.length; i++) {
			cost *= ((double) sizes[i] / rd[i]);
		}
		return cost;
	}

	@Override
	public int compare(Assignment o1, Assignment o2) {
		return -Double.compare(computationCost(o1.sizes, o1.rd), computationCost(o2.sizes, o2.rd));
	}
	
}

/**
 * Communication cost: the number of tuples each machine receives
 */
class CommCost implements Comparator<Assignment>, Serializable {
	
	private double communicationCost(long[] sizes, int[] rd) {
		double cost = 0;
		for (int i = 0; i < sizes.length; i++) {
			cost += ((double) sizes[i] / rd[i]);
		}
		return cost;
	}

	@Override
	public int compare(Assignment o1, Assignment o2) {
		return -Double.compare(communicationCost(o1.sizes, o1.rd), communicationCost(o2.sizes, o2.rd));
	}
	
}

/**
 * A partitioning solution
 */
class Assignment implements Serializable{
	int[] rd;
	long[] sizes;
	public Assignment (long[] _sizes, int[] _rd){
		rd = _rd;
		sizes = _sizes;
	}
}

