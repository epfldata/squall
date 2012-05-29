/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/*
 * If the list of all possible hash values isspecified,
 *   it uses uniform key (not number of tuples!) distribution
 * Otherwise, we encode fieldGrouping exactly the same as the Storm authors.
 * Because of NoACK possibility, have to be used everywhere in the code.
 */
public class BalancedStreamGrouping implements CustomStreamGrouping{

    //the number of tasks on the level this stream grouping is sending to
    private int _numTasks;
    
    private List<String> _fullHashList;

    private Map _map;

    public BalancedStreamGrouping(Map map) {
        _map = map;
    }

    public BalancedStreamGrouping(Map map, List<String> fullHashList){
        _map = map;
        _fullHashList = fullHashList;
    }

    @Override
    public void prepare(Fields fields, int numTasks) {
        _numTasks = numTasks;
    }

    @Override
    public List<Integer> taskIndices(List<Object> stormTuple) {
        List<String> tuple = (List<String>) stormTuple.get(1);
        String tupleHash = (String) stormTuple.get(2);
        if(MyUtilities.isFinalAck(tuple, _map)){
            List<Integer> result = new ArrayList<Integer>();
            for(int i=0; i< _numTasks; i++){
                result.add(i);
            }
            return result;
        }
        if(!isBalanced()){
            return fieldGrouping(tupleHash);
        }else{
            return balancedGrouping(tupleHash);
        }
    }

    private boolean isBalanced(){
        return (_fullHashList != null);
    }

    private List<Integer> fieldGrouping(String tupleHash){
        int index = Math.abs(tupleHash.hashCode()) % _numTasks;
        return Arrays.asList(index);
    }

    private List<Integer> balancedGrouping(String tupleHash){
        int index = _fullHashList.indexOf(tupleHash) % _numTasks;
        return Arrays.asList(index);
    }

}
