/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import utilities.DeepCopy;
import utilities.MyUtilities;


public class JoinAggStorage {
    private static final long serialVersionUID = 1L;

    private Map<String, AggregateOperator> _storage = new HashMap<String, AggregateOperator>();
    private AggregateOperator _agg;
    private Map _conf;
    
    public JoinAggStorage(AggregateOperator agg, Map conf){
        _agg = agg;
        _conf = conf;
    }

    public List<String> get(String tupleHash) {
        //have to return complete content of _agg
        AggregateOperator agg = _storage.get(tupleHash);
        if (agg == null){
            return null;
        }else{
            return agg.getContent();
        }
    }

    public void put(String tupleHash, String tupleString) {
        AggregateOperator agg = _storage.get(tupleHash);
        if(agg == null){
            agg = (AggregateOperator) DeepCopy.copy(_agg);
            _storage.put(tupleHash, agg);
        }
        List<String> tuple = MyUtilities.stringToTuple(tupleString, _conf);
        agg.process(tuple);
    }
}
