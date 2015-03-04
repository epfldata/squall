package plan_runner.components;

import java.io.Serializable;
import java.util.List;

import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;

public interface ComponentProperties extends Serializable {

	public List<DataSourceComponent> getAncestorDataSources();

	public long getBatchOutputMillis();

	public ChainOperator getChainOperator(); // contains all the previously
	// added operators

	// TODO: problem when having multiple children (sharing scenarios)
	public Component getChild();

	public List<String> getFullHashList();

	public List<ValueExpression> getHashExpressions();

	public List<Integer> getHashIndexes();

	public String getInfoID();

	public String getName();

	public Component[] getParents();

	public boolean getPrintOut();

}
