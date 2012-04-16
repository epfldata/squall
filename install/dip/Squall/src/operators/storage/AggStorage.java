/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators.storage;

import java.io.Serializable;
import java.util.List;


public interface AggStorage<T> extends Serializable{
    public T get(String key);
    public void put(String key, T value);

    public T updateContent(List<String> tuple, String tupleHash);
    public T updateContent(T value, String key);

    //returns the internal data structure
    public Object getAll();

    //add the content of the other storage
    public void addContent(AggStorage otherStorage);
    
    //print the content in user-readable form
    public String printContent();
    
    //get the content in machine readable form - used in preaggregations
    public List<String> getContent();

}
