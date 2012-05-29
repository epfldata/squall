/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import java.util.ArrayList;
import java.util.List;


public class ChainOperator implements Operator {

    private List<Operator> _operators = new ArrayList<Operator>();

    public ChainOperator(Operator... opArray){
        for(Operator oper: opArray){
            if(oper!=null){
                _operators.add(oper);
            }
        }
    }

    public ChainOperator(List<Operator> operators){
        _operators = operators;
    }

    //we can creat an empty chainOperator and later fill it in
    public ChainOperator(){

    }

    /*
     * Delete the previously added operators and add new list of operators
     */
    public void setOperators(List<Operator> operators){
        _operators = operators;
    }

    /*
     * Add an operator to the tail
     */
    public void addOperator(Operator operator){
        _operators.add(operator);
    }

    public List<Operator> getOperators(){
        return _operators;
    }

    public Operator getLastOperator(){
        if(size()>0){
            return _operators.get(size()-1);
        }else{
            return null;
        }
    }

    //******************************************************
    
    /*
     * return first appearance of SelectOperator
     * used when ordering operators in Simple and rule-based optimizer
     */
    public SelectOperator getSelection(){
        for(Operator op:_operators){
            if (op instanceof SelectOperator) return (SelectOperator) op;
        }
        return null;
    }

    /*
     * return first appearance of DistinctOperator
     * used when ordering operators in Simple and rule-based optimizer
     */
    public DistinctOperator getDistinct(){
        for(Operator op:_operators){
            if (op instanceof DistinctOperator) return (DistinctOperator) op;
        }
        return null;
    }

    /*
     * return first appearance of ProjectOperator
     * used when ordering operators in Simple and rule-based optimizer
     * used in rule-based optimizer
     */
    public ProjectOperator getProjection(){
        for(Operator op:_operators){
            if (op instanceof ProjectOperator) return (ProjectOperator) op;
        }
        return null;
    }

    /*
     * return first appearance of AggregationOperator
     * used when ordering operators in Simple and rule-based optimizer
     * used in rule-based optimizer
     */
    public AggregateOperator getAggregation(){
        for(Operator op:_operators){
            if (op instanceof AggregateOperator) return (AggregateOperator) op;
        }
        return null;
    }
    //******************************************************

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
        if(getLastOperator()!=null){
            return getLastOperator().isBlocking();
        }else{
            return false;
        }
    }

    @Override
    public int getNumTuplesProcessed(){
        if(isBlocking()){
            return getLastOperator().getNumTuplesProcessed();
        }else{
            throw new RuntimeException("tuplesProcessed for non-blocking last operator should never be invoked!");
        }
    }    
    
    private int size(){
        return _operators.size();
    }

    @Override
    public String printContent() {
        String result = null;
        if(isBlocking()){
            result = getLastOperator().printContent();
        }
        return result;
    }

    @Override
    public List<String> getContent() {
        List<String> result = null;
        if(isBlocking()){
            result = getLastOperator().getContent();
        }
        return result;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("ChainOperator contains the following operators(in this order):\n");
        for(Operator op:_operators){
            sb.append(op).append("\n");
        }
        return sb.toString();
    }

}