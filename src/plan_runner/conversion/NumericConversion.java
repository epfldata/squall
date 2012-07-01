/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.conversion;


public interface NumericConversion<T extends Number> extends TypeConversion<T> {

    public T fromDouble(double d);
    public double toDouble(T obj);

}
