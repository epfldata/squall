package ch.epfl.data.squall.conversion;

import java.io.Serializable;

public class SumCount implements Comparable<SumCount>, Serializable {
	private static final long serialVersionUID = 1L;

	private Double _sum;
	private Long _count;

	public SumCount(Double sum, Long count) {
		_sum = sum;
		_count = count;
	}

	@Override
	public int compareTo(SumCount other) {
		if (getAvg() > other.getAvg())
			return 1;
		else if (getAvg() < other.getAvg())
			return -1;
		else
			return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof SumCount))
			return false;
		final SumCount otherSumCount = (SumCount) obj;
		return getAvg() == otherSumCount.getAvg();
	}

	public double getAvg() {
		return _sum / _count;
	}

	public Long getCount() {
		return _count;
	}

	public Double getSum() {
		return _sum;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 89 * hash + (_sum != null ? _sum.hashCode() : 0);
		hash = 89 * hash + (_count != null ? _count.hashCode() : 0);
		return hash;
	}

	public void setCount(Long count) {
		_count = count;
	}

	public void setSum(Double sum) {
		_sum = sum;
	}
}