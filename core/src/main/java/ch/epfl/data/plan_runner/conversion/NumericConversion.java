package ch.epfl.data.plan_runner.conversion;

public interface NumericConversion<T extends Number> extends TypeConversion<T> {

	public T fromDouble(double d);

	public T getMaxValue();

	public T getMinPositiveValue();

	public T getMinValue();

	public T getOffset(Object base, double delta);

	public T minDecrement(Object obj);

	public T minIncrement(Object obj);

	public double toDouble(Object obj);

}
