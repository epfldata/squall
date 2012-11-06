package sql.optimizers.name.manual_batching;

import sql.optimizers.name.*;
import java.util.*;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.utilities.SystemParameters;
import sql.schema.Schema;
import sql.util.ImproperParallelismException;
import sql.util.TableAliasName;


public class ManualBatchingParallelismAssigner extends CostParallelismAssigner {

    private static double MAX_LATENCY_MILLIS = 50000;
    private static int MAX_COMP_PAR = 220;
    
    public ManualBatchingParallelismAssigner(Schema schema, TableAliasName tan, Map map) {
        super(schema, tan, map);
    }
    
    
    //SOURCES
    @Override
    protected int parallelismFormula(DataSourceComponent source){
        int parallelism = -1;
        String compName = source.getName();
        if(SystemParameters.isExisting(_map, "DIP_BATCH_PAR") &&
                SystemParameters.getBoolean(_map, "DIP_BATCH_PAR")){
            if(SystemParameters.isExisting(_map, compName+ "_PAR")){
                //a user provides parallelism explicitly
                parallelism = SystemParameters.getInt(_map, compName + "_PAR");
            }
        }
        if(parallelism == -1){
            //if there is no provided parallelism of a source, resort to superclass way of assigning parallelism
            return super.parallelismFormula(source);
        }else{
            return parallelism;
        }
    }
    
    //this method also set latency for useful work
    @Override
    protected void setBatchSize(DataSourceComponent source, Map<String, CostParams> compCost) {
        CostParams params = compCost.get(source.getName());
        int batchSize = (int) (SystemParameters.getInt(_map, "BATCH_SIZE") * params.getSelectivity());
        if(batchSize < 1){
            batchSize = 1; //cannot be less than 1
        }
        params.setBatchSize(batchSize);
        double latency = batchSize * ClusterConstants.getReadTime();
        params.setLatency(latency); // this is only due to useful work
        params.setTotalAvgLatency(latency);
    }
    
    
    //JOINS
    //this method also set latency for rcv + useful work for the join component
    //TODO: should check if the parallelism is bottleneck
    @Override
    protected int parallelismFormula(String compName, CostParams params, CostParams leftParentParams, CostParams rightParentParams) {
        //TODO: this formula does not take into account when joinComponent send tuples further down
        //TODO: we should also check for bottlenecks
        double minLatency = MAX_LATENCY_MILLIS;
        int parallelism = -1;
        
        if(SystemParameters.isExisting(_map, "DIP_BATCH_PAR") &&
                SystemParameters.getBoolean(_map, "DIP_BATCH_PAR")){
            if(SystemParameters.isExisting(_map, compName+ "_PAR")){
                parallelism = SystemParameters.getInt(_map, compName + "_PAR");
            }else{
                //I don't want this query plan
                throw new ImproperParallelismException("A user did not specify parallelism for " + compName +
                        ". Thus, it is assumed he does not want that query plan!");
            }
        }else{
            //we start from the sum of parent parallelism (we know it won't be less anyway)
            int minParallelism = leftParentParams.getParallelism() + rightParentParams.getParallelism();
            for(int i = minParallelism; i < MAX_COMP_PAR; i++){
                double latency = estimateJoinLatency(i, leftParentParams, rightParentParams);
                if(latency < minLatency){
                    minLatency = latency;
                    parallelism = i;
                }
            }
        }
        updateJoinLatencies(parallelism, params, leftParentParams, rightParentParams);
        return parallelism;
    }
    
    //at the moment of invoking this, parallelism is not yet put in costParams of the component
    private void updateJoinLatencies(int parallelism, CostParams params, CostParams leftParentParams, CostParams rightParentParams) {
        //left parent
        double leftSndTime = estimateSndTimeLeftParent(parallelism, leftParentParams);
        leftParentParams.setLatency(leftParentParams.getLatency() + leftSndTime);
        double leftTotalAvgLatency = leftParentParams.getTotalAvgLatency() + leftSndTime;
        leftParentParams.setTotalAvgLatency(leftTotalAvgLatency);
        
        //right parent
        double rightSndTime = estimateSndTimeRightParent(parallelism, rightParentParams);
        rightParentParams.setLatency(rightParentParams.getLatency() + rightSndTime);
        double rightTotalAvgLatency = rightParentParams.getTotalAvgLatency() + rightSndTime;
        rightParentParams.setTotalAvgLatency(rightTotalAvgLatency);
        
        //this component sets latency only due to rcv and uw
        double rcvTime = estimateJoinRcvTime(parallelism, leftParentParams, rightParentParams);
        double uwTime = estimateJoinUsefullLatency(parallelism, leftParentParams, rightParentParams);
        params.setLatency(rcvTime + uwTime);
        
        //update total latency for this component
        long leftCardinality = leftParentParams.getCardinality();
        long rightCardinality = rightParentParams.getCardinality();
        double totalAvgParentLatency = 
                (leftTotalAvgLatency * leftCardinality + rightTotalAvgLatency * rightCardinality) / (leftCardinality + rightCardinality);
        double totalAvgLatency = totalAvgParentLatency + rcvTime + uwTime;
        params.setTotalAvgLatency(totalAvgLatency);
    }
    
    //this adds 
    //1. sndtime from the parents
    //2. rcvtime form this component
    //3. usefull work from this component
    //      TODO: sndtime of this component is not accounted for
    private double estimateJoinLatency(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        double sndTimeParent = estimateSndTimeParents(parallelism, leftParentParams, rightParentParams);
        double rcvTime = estimateJoinRcvTime(parallelism, leftParentParams, rightParentParams);
        double uwTime = estimateJoinUsefullLatency(parallelism, leftParentParams, rightParentParams);
        return sndTimeParent + rcvTime + uwTime;
    }    
       
    @Override
    protected void setBatchSize(EquiJoinComponent joinComponent, Map<String, CostParams> compCost) {
        Component[] parents = joinComponent.getParents();
        CostParams leftParams = compCost.get(parents[0].getName());
        CostParams rightParams = compCost.get(parents[1].getName());
        CostParams params = compCost.get(joinComponent.getName());
        
        double ratio = params.getSelectivity();
        int parallelism = params.getParallelism(); // batch size has to be set after the parallelism
        int leftBatchSize = leftParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        int rightBatchSize = rightParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        int leftParallelism = leftParams.getParallelism();
        int rightParallelism = rightParams.getParallelism();
        
        //TODO: this implies that both components finish at the same time (optimization of parallelism of sources won't work)
        double iqs = leftParallelism * leftBatchIn + rightParallelism * rightBatchIn;
        int batchSize = (int) (ratio * iqs);
        if(batchSize < 1){
            batchSize = 1; //cannot be less than 1
        }
        params.setBatchSize(batchSize);
    }
    
    
    //OPERATORS
    @Override
    public void setParallelism(OperatorComponent opComp, Map<String, CostParams> compCost) {    
        super.setParallelism(opComp, compCost);
        
        CostParams params = compCost.get(opComp.getName());
        int parallelism = params.getParallelism();
        CostParams parentParams = compCost.get(opComp.getParents()[0].getName());
        updateOpLatencies(parallelism, params, parentParams);
    }
    
    private void updateOpLatencies(int parallelism, CostParams params, CostParams parentParams) {
        //parent
        double parentSndTime = estimateSndTimeLeftParent(parallelism, parentParams);
        parentParams.setLatency(parentParams.getLatency() + parentSndTime);
        double parentTotalAvgLatency = parentParams.getTotalAvgLatency() + parentSndTime;
        parentParams.setTotalAvgLatency(parentTotalAvgLatency);
        
        //this component sets latency only due to rcv and uw
        double rcvTime = estimateOpRcvTime(parallelism, parentParams);
        double uwTime = estimateOpUsefullLatency(parallelism, parentParams);
        params.setLatency(rcvTime + uwTime);
        
        //update total latency for this component
        double totalAvgLatency = parentTotalAvgLatency + rcvTime + uwTime;
        params.setTotalAvgLatency(totalAvgLatency);
    }    
    
    
    //HELPER methods
    private double estimateSndTimeParents(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        long leftCardinality = leftParentParams.getCardinality();//after all the operators, including selections, are applied to parents
        long rightCardinality = rightParentParams.getCardinality();
        
        double leftSndTime = estimateSndTimeLeftParent(parallelism, leftParentParams);
        double rightSndTime = estimateSndTimeRightParent(parallelism, rightParentParams);
        
        //TODO: we combine them linerly based on number of tuples, parallelisms, batch sizes etc.
        //for now only cardinality is taken into account
        return (leftSndTime * leftCardinality + rightSndTime * rightCardinality) / (leftCardinality + rightCardinality);
    }

    private double estimateSndTimeLeftParent(int parallelism, CostParams leftParentParams) {
        int leftBatchSize = leftParentParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        
        return (((double)parallelism + 1) / 2) * ClusterConstants.getSerTime(leftBatchIn);
    }

    private double estimateSndTimeRightParent(int parallelism, CostParams rightParentParams) {
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        
        return (((double)parallelism + 1) / 2) * ClusterConstants.getSerTime(rightBatchIn);
    }    
    
    private double estimateJoinRcvTime(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        int leftBatchSize = leftParentParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        int leftParallelism = leftParentParams.getParallelism();
        int rightParallelism = rightParentParams.getParallelism();
        
        return leftParallelism * ClusterConstants.getDeserTime(leftBatchIn) + 
                rightParallelism * ClusterConstants.getDeserTime(rightBatchIn);
        
    }
    
    private double estimateJoinUsefullLatency(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        int leftBatchSize = leftParentParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        int leftParallelism = leftParentParams.getParallelism();
        int rightParallelism = rightParentParams.getParallelism();
        
        double iqs = leftParallelism * leftBatchIn + rightParallelism * rightBatchIn;
        return  ClusterConstants.getJoinTime() * iqs;
    }
    
    private double estimateOpRcvTime(int parallelism, CostParams parentParams) {
        int parentBatchSize = parentParams.getBatchSize();
        int parentBatchIn = parentBatchSize / parallelism;
        int parentParallelism = parentParams.getParallelism();
        
        return parentParallelism * ClusterConstants.getDeserTime(parentBatchIn);
    }
    
    private double estimateOpUsefullLatency(int parallelism, CostParams parentParams) {
        int parentBatchSize = parentParams.getBatchSize();
        int parentBatchIn = parentBatchSize / parallelism;
        int parentParallelism = parentParams.getParallelism();
        
        double iqs = parentParallelism * parentBatchIn;
        return  ClusterConstants.getOpTime() * iqs;
    }    
}