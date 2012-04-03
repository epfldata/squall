/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import java.util.List;
import java.util.Map;
import predicates.Predicate;

public class SelectionOperator implements Operator {
    private static final long serialVersionUID = 1L;
    
    private Predicate _predicate;

    public SelectionOperator(Predicate predicate){
        _predicate = predicate;
    }

    public Predicate getPredicate(){
        return _predicate;
    }

    @Override
    public List<String> process(List<String> tuple) {
        if (_predicate.test(tuple)){
            return tuple;
        }else{
            return null;
        }
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @Override
    public String printContent() {
        throw new RuntimeException("printContent for SelectionOperator should never be invoked!");
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("SelectionOperator with Predicate: ");
        sb.append(_predicate.toString());
        return sb.toString();
    }
}
