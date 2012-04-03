/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package conversion;

import java.io.Serializable;


public interface TypeConversion<T> extends Serializable{
    public T fromString(String str);
    public String toString(T obj);

    public T getInitialValue();
}