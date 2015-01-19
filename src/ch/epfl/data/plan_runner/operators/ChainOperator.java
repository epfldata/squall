package ch.epfl.data.plan_runner.operators;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.data.plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.plan_runner.visitors.OperatorVisitor;

public class ChainOperator implements Operator {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private List<Operator> _operators = new ArrayList<Operator>();

    // we can create an empty chainOperator and later fill it in
    public ChainOperator() {

    }

    public ChainOperator(List<Operator> operators) {
	_operators = operators;
    }

    public ChainOperator(Operator... opArray) {
	for (final Operator oper : opArray)
	    if (oper != null)
		_operators.add(oper);
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    /*
     * Add an operator to the tail
     */
    public void addOperator(Operator operator) {
	if (operator != null) {
	    _operators.add(operator);
	}
    }

    /*
     * return first appearance of AggregationOperator used when ordering
     * operators in Simple and rule-based optimizer used in rule-based optimizer
     */
    public AggregateOperator getAggregation() {
	for (final Operator op : _operators)
	    if (op instanceof AggregateOperator)
		return (AggregateOperator) op;
	return null;
    }

    @Override
    public List<String> getContent() {
	List<String> result = null;
	if (isBlocking())
	    result = getLastOperator().getContent();
	return result;
    }

    // ******************************************************

    /*
     * return first appearance of DistinctOperator used when ordering operators
     * in Simple and rule-based optimizer
     */
    public DistinctOperator getDistinct() {
	for (final Operator op : _operators)
	    if (op instanceof DistinctOperator)
		return (DistinctOperator) op;
	return null;
    }

    public Operator getLastOperator() {
	if (size() > 0)
	    return _operators.get(size() - 1);
	else
	    return null;
    }

    @Override
    public int getNumTuplesProcessed() {
	if (isBlocking())
	    return getLastOperator().getNumTuplesProcessed();
	else
	    throw new RuntimeException(
		    "tuplesProcessed for non-blocking last operator should never be invoked!");
    }

    public List<Operator> getOperators() {
	return _operators;
    }

    // ******************************************************

    /*
     * return first appearance of ProjectOperator used when ordering operators
     * in Simple and rule-based optimizer used in rule-based optimizer
     */
    public ProjectOperator getProjection() {
	for (final Operator op : _operators)
	    if (op instanceof ProjectOperator)
		return (ProjectOperator) op;
	return null;
    }

    /*
     * return first appearance of SelectOperator used when ordering operators in
     * Simple and rule-based optimizer
     */
    public SelectOperator getSelection() {
	for (final Operator op : _operators)
	    if (op instanceof SelectOperator)
		return (SelectOperator) op;
	return null;
    }

    public PrintOperator getPrint() {
	for (final Operator op : _operators)
	    if (op instanceof PrintOperator)
		return (PrintOperator) op;
	return null;
    }

    public SampleAsideAndForwardOperator getSampleAside() {
	for (final Operator op : _operators)
	    if (op instanceof SampleAsideAndForwardOperator)
		return (SampleAsideAndForwardOperator) op;
	return null;
    }

    // closing the files of the printOperator
    public void finalizeProcessing() {
	PrintOperator printOperator = getPrint();
	if (printOperator != null) {
	    printOperator.finalizeProcessing();
	}
    }

    @Override
    public boolean isBlocking() {
	if (getLastOperator() != null)
	    return getLastOperator().isBlocking();
	else
	    return false;
    }

    public boolean isEmpty() {
	return size() == 0;
    }

    @Override
    public String printContent() {
	String result = null;
	if (isBlocking())
	    result = getLastOperator().printContent();
	return result;
    }

    /*
     * Return tuple if the tuple has to be sent further Otherwise return null.
     */
    @Override
    public List<String> process(List<String> tuple) {
	List<String> result = tuple;

	for (final Operator operator : _operators) {
	    result = operator.process(result);
	    if (result == null)
		break;
	}
	return result;
    }

    /*
     * Delete the previously added operators and add new list of operators
     */
    public void setOperators(List<Operator> operators) {
	_operators = operators;
    }

    public int size() {
	return _operators.size();
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	for (final Operator op : _operators)
	    sb.append(op).append("\n");
	return sb.toString();
    }
}
