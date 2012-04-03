/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package stormComponents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JoinHashStorage implements JoinStorage {
    private static final long serialVersionUID = 1L;

    //multiple tuples per key possible
    private Map<String, List<String>> _storage = new HashMap<String, List<String>>();

    public List<String> get(String tupleHash) {
        return _storage.get(tupleHash);
    }

    public void put(String tupleHash, String tupleString) {
        List<String> stringTupleList = _storage.get(tupleHash);
        if(stringTupleList == null){
            stringTupleList = new ArrayList<String>();
            _storage.put(tupleHash, stringTupleList);
        }
        stringTupleList.add(tupleString);
    }

}