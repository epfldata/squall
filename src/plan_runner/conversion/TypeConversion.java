package plan_runner.conversion;

import java.io.Serializable;

public interface TypeConversion<T> extends Serializable {
	public T fromString(String str);

	public double getDistance(T bigger, T smaller);

	public T getInitialValue();

	public String toString(T obj);
}