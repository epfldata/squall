package sql.optimizers.name.manual_batching;

import sql.optimizers.name.*;
import java.util.Map;
import sql.schema.Schema;
import sql.util.TableAliasName;

/*
 * It generates different NameCompGen for each partial query plan
 *   NameCompGen is responsible for attaching operators to components
 * Aggregation only on the last level.
 */
public class ManualBatchingCompGenFactory {
    private Schema _schema;
    private Map _map; //map is updates in place
    private TableAliasName _tan;
    
    private ManualBatchingParallelismAssigner _parAssigner;
    
    /*
     * only plan, no parallelism
     */
    public ManualBatchingCompGenFactory(Map map, TableAliasName tan){
        _map = map;
        _tan = tan;
        
        _schema = new Schema(map);
    }
    
    /*
     * generating plan + parallelism
     */
    public ManualBatchingCompGenFactory(Map map, TableAliasName tan, int totalSourcePar){
        this(map, tan);
        setParAssignerMode(totalSourcePar);
    }
    
    public final void setParAssignerMode(int totalSourcePar){
        //in general there might be many NameComponentGenerators, 
        //  that's why CPA is computed before of NCG
        _parAssigner = new ManualBatchingParallelismAssigner(_schema, _tan, _map);
        
        //for the same _parAssigner, we might try with different totalSourcePar
        _parAssigner.computeSourcePar(totalSourcePar);
    }
    
    public NameCompGen create(){        
        return new NameCompGen(_schema, _map, _parAssigner);
    }
    
    public ManualBatchingParallelismAssigner getParAssigner(){
        return _parAssigner;
    }

}