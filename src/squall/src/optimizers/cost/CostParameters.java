/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import java.util.List;


public class CostParameters {

    //unless otherwise specified, there is no pruning tuples from a relation
    private double _selectivity = 1.0;
    private long _cardinality;
    private int _parallelism;
    private List<String> _schema;

    /**
     * @return the _selectivity
     */
    public double getSelectivity() {
        return _selectivity;
    }

    /**
     * @param selectivity the _selectivity to set
     */
    public void setSelectivity(double selectivity) {
        _selectivity = selectivity;
    }

    /**
     * @return the _cardinality
     */
    public long getCardinality() {
        return _cardinality;
    }

    /**
     * @param cardinality the _cardinality to set
     */
    public void setCardinality(long cardinality) {
        _cardinality = cardinality;
    }

    /**
     * @return the _parallelism
     */
    public int getParallelism() {
        return _parallelism;
    }

    /**
     * @param parallelism the _parallelism to set
     */
    public void setParallelism(int parallelism) {
        _parallelism = parallelism;
    }

    /**
     * @return the _schema
     */
    public List<String> getSchema() {
        return _schema;
    }

    /**
     * @param schema the _schema to set
     */
    public void setSchema(List<String> schema) {
        _schema = schema;
    }

}
