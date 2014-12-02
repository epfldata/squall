package plan_runner.conversion;

public interface NumericConversion<T extends Number> extends TypeConversion<T> {

	public T getMinValue();
	
	public T getMinPositiveValue();
	
	public T minIncrement(Object obj);
	
	public T minDecrement(Object obj);	
	
	public T getMaxValue();
	
	public T fromDouble(double d);

	public double toDouble(Object obj);

	public T getOffset(Object base, double delta);

}
