/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import java.util.Arrays;
import java.util.List;

/*
 * If the list of all possible hash values isspecified,
 *   it uses uniform key (not number of tuples!) distribution
 * Otherwise, we encode fieldGrouping exactly the same as the Storm authors
 */
public class BalancedStreamGrouping implements CustomStreamGrouping{

    //the number of tasks on the level this stream grouping is sending to
    private int _numTasks;
    
    private List<String> _fullHashList;

    public BalancedStreamGrouping() {
        
    }

    public BalancedStreamGrouping(List<String> fullHashList){
        _fullHashList = fullHashList;
    }

    @Override
    public void prepare(Fields fields, int numTasks) {
        _numTasks = numTasks;
    }

    @Override
    public List<Integer> taskIndices(List<Object> stormTuple) {
        String tupleHash = (String) stormTuple.get(2);
        if(isBalanced()){
            return balancedGrouping(tupleHash);
        }else{
            return fieldGrouping(tupleHash);
        }
    }

    private boolean isBalanced(){
        return (_fullHashList != null);
    }

    private List<Integer> balancedGrouping(String tupleHash){
        int index = _fullHashList.indexOf(tupleHash) % _numTasks;
        return Arrays.asList(index);
    }

    private List<Integer> fieldGrouping(String tupleHash){
        int index = Math.abs(tupleHash.hashCode()) % _numTasks;
        return Arrays.asList(index);
    }

}
