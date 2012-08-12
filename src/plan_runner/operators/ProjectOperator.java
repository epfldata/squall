package plan_runner.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import plan_runner.visitors.OperatorVisitor;

public class ProjectOperator implements Operator {
	private static final long serialVersionUID = 1L;

        private List<ValueExpression> _veList = new ArrayList<ValueExpression>();

        private int _numTuplesProcessed = 0;

        public ProjectOperator(ValueExpression... veArray){
            _veList.addAll(Arrays.asList(veArray));
        }

        public ProjectOperator(List<ValueExpression> veList){
            _veList = veList;
        }

        public ProjectOperator(int[] projectIndexes){
            for(int columnNumber: projectIndexes){
                ColumnReference columnReference = new ColumnReference(new StringConversion(), columnNumber);
                _veList.add(columnReference);
            }
	}

        public List<ValueExpression> getExpressions(){
            return _veList;
        }

        @Override
	public List<String> process(List<String> tuple) {
            _numTuplesProcessed++;
            List<String> projection = new ArrayList<String>();
            for(ValueExpression ve: _veList){
                String columnContent = ve.evalString(tuple);
                projection.add(columnContent);
            }
            return projection;
        }

        @Override
        public boolean isBlocking() {
            return false;
        }

        @Override
        public String printContent() {
            throw new RuntimeException("printContent for ProjectionOperator should never be invoked!");
        }

        @Override
        public int getNumTuplesProcessed(){
            return _numTuplesProcessed;
        }

        @Override
        public List<String> getContent() {
            throw new RuntimeException("getContent for ProjectionOperator should never be invoked!");
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("ProjectOperator (");
            for(int i=0; i<_veList.size();i++){
                sb.append(_veList.get(i).toString());
                if(i==_veList.size()-1){
                    sb.append(")");
                }else{
                    sb.append(", ");
                }
            }
            return sb.toString();
        }

        @Override
        public void accept(OperatorVisitor ov){
            ov.visit(this);
        }
}