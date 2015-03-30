package ch.epfl.data.squall.ewh.data_structures;

import java.util.List;

public class BooleanRegions {
	private boolean _satisfied;
	private List<Region> _regions;

	public BooleanRegions(boolean satisfied, List<Region> regions) {
		_satisfied = satisfied;
		_regions = regions;
	}

	public List<Region> getRegions() {
		return _regions;
	}

	public boolean isSatisfied() {
		return _satisfied;
	}

	public void setRegions(List<Region> regions) {
		_regions = regions;
	}

	public void setSatisfied(boolean satisfied) {
		_satisfied = satisfied;
	}
}