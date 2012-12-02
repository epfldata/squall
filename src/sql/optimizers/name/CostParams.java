package sql.optimizers.name;

import java.io.Serializable;
import sql.util.TupleSchema;

/*
 * parallelism also has to be inside,
 *   we cannot use Config maps to exchange parallelism,
 *   because there are many query plans building in parallel (Selinger-style optimization)
 * From the final choosen query plan, we fill in config maps.
 */
public class CostParams implements Serializable{
    private static final long serialVersionUID = 1L;

    //unless otherwise specified, there is no pruning tuples from a relation
    private double _selectivity = 1.0;
    private long _cardinality; //total number of tuples at the output of a component
    private TupleSchema _schema;
    private int _parallelism;
    
    // only used in manual batching
    private int _batchSize; 
    private double _latency; //in millis
    private double _totalAvgLatency; //in millis

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
    public TupleSchema getSchema() {
        return _schema;
    }

    /**
     * @param schema the _schema to set
     */
    public void setSchema(TupleSchema schema) {
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
        _parallelism = parallelism;
    }

    /**
     * @return the _batchSize
     */
    public int getBatchSize() {
        return _batchSize;
    }

    /**
     * @param batchSize the _batchSize to set
     */
    public void setBatchSize(int batchSize) {
        _batchSize = batchSize;
    }

    /**
     * @return the _latency
     */
    public double getLatency() {
        return _latency;
    }

    /**
     * @param latency the _latency to set
     */
    public void setLatency(double latency) {
        _latency = latency;
    }

    /**
     * @return the _totalAvgLatency
     */
    public double getTotalAvgLatency() {
        return _totalAvgLatency;
    }

    /**
     * @param totalAvgLatency the _totalAvgLatency to set
     */
    public void setTotalAvgLatency(double totalAvgLatency) {
        _totalAvgLatency = totalAvgLatency;
    }
    
}
