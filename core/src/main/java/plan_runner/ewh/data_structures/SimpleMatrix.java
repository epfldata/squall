package plan_runner.ewh.data_structures;

//used for prefixSum in DenseMonotonicWeightPrecomputation and PWeightPrecomputation
public interface SimpleMatrix {
	
	// max num elements to store
	public long getCapacity();
	
	// num of elements stored (in the case of dense implementation (e.g. int[][]), returns x * y
	public long getNumElements();
	
	public int getXSize();
	public int getYSize();

	public int getElement(int x, int y);
	public void setElement(int value, int x, int y);
	public void increment(int x, int y);
	public void increase(int delta, int x, int y);
	
}