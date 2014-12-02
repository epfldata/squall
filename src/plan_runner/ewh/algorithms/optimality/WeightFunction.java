package plan_runner.ewh.algorithms.optimality;

public class WeightFunction {

	private double _a, _b;
	
	public WeightFunction(double a, double b){
		_a = a;
		_b = b;
	}
	
	public double getWeight(int halfPerimeter, int frequency){
		return _a * halfPerimeter + _b * frequency;
	}
	
	@Override
	public String toString(){
		return "a = " + _a + ", b = " + _b;
	}

	public double getA() {
		// TODO Auto-generated method stub
		return 0;
	}

	public double getB() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
