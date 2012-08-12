package plan_runner.conversion;

import java.io.Serializable;


public interface TypeConversion<T> extends Serializable{
    public T fromString(String str);
    public String toString(T obj);

    public T getInitialValue();

    public double getDistance(T bigger, T smaller);
}