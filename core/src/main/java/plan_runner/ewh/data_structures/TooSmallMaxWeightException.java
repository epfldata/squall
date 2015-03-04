package plan_runner.ewh.data_structures;

import plan_runner.ewh.algorithms.ShallowCoarsener;

public class TooSmallMaxWeightException extends Exception {
	private static final long serialVersionUID = 1L;
	private double _maxWeight;
	private ShallowCoarsener _coarsener;
	
	public TooSmallMaxWeightException(double maxWeight, ShallowCoarsener coarsener){
		_maxWeight = maxWeight;
		_coarsener = coarsener;
	}
	
	public String toString(){
		return "Impossible to achieve maxWeight less than " + _maxWeight + " with " + (_coarsener != null ? _coarsener: "");
	}

}