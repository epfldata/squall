/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ChainOperator implements Operator {

    private ArrayList<Operator> _operators = new ArrayList<Operator>();

    public ChainOperator(Operator... opArray){
        for(Operator oper: opArray){
            if(oper!=null){
                _operators.add(oper);
            }
        }
    }

    public ChainOperator(ArrayList<Operator> operators){
        _operators = operators;
    }
    
    /* Return tuple if the tuple has to be sent further
     *   Otherwise return null.
     */
    @Override
    public List<String> process(List<String> tuple) {
        List<String> result = tuple;
        for(Operator operator: _operators){
             result = operator.process(result);
             if(result == null){
                 break;
             }
        }
        return result;
    }

    @Override
    public boolean isBlocking() {
        if(lastOperator()!=null){
            return lastOperator().isBlocking();
        }else{
            return false;
        }
    }

    @Override
    public String printContent() {
        String result = null;
        if(isBlocking()){
            result = lastOperator().printContent();
        }
        return result;
    }


    private Operator lastOperator(){
        if(size()>0){
            return _operators.get(size()-1);
        }else{
            return null;
        }
    }

    private int size(){
        return _operators.size();
    }

}