/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import java.util.List;

/*
 * parallelism also has to be inside,
 *   we cannot use Config maps to exchange parallelism,
 *   because there are many query plans building in parallel (Selinger-style optimization)
 * From the final choosen query plan, we fill in config maps.
 */
public class CostParams {

    //unless otherwise specified, there is no pruning tuples from a relation
    private double _selectivity = 1.0;
    private long _cardinality; //total number of tuples at the output of a component
    private List<String> _schema;
    private int _parallelism;

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
        this._parallelism = parallelism;
    }

}
