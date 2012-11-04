package sql.optimizers.name.manual_batching;

import sql.optimizers.name.*;
import java.util.*;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import sql.schema.Schema;
import sql.util.TableAliasName;


public class ManualBatchingParallelismAssigner extends CostParallelismAssigner {

    private static int SOURCE_BATCH_SIZE = 1024;
    private static double MAX_LATENCY_MILLIS = 50000;
    private static int MAX_COMP_PAR = 220;
    
    public ManualBatchingParallelismAssigner(Schema schema, TableAliasName tan, Map map) {
        super(schema, tan, map);
    }
    
    
    //SOURCES
    //this method also set latency for useful work
    @Override
    protected void setBatchSize(DataSourceComponent source, Map<String, CostParams> compCost) {
        CostParams params = compCost.get(source.getName());
        int batchSize = SOURCE_BATCH_SIZE;
        params.setBatchSize(batchSize);
        double latency = batchSize * Constants.getReadTime();
        params.setLatency(latency); // this is only due to useful work
        params.setTotalAvgLatency(latency);
    }
    
    
    //JOINS
    //this method also set latency for rcv + useful work for the join component
    //TODO: should check if the parallelism is bottleneck
    @Override
    protected int parallelismFormula(CostParams params, CostParams leftParentParams, CostParams rightParentParams) {
        //TODO: this formula does not take into account when joinComponent send tuples further down
        //TODO: we should also check for bottlenecks
        double minLatency = MAX_LATENCY_MILLIS;
        int parallelism = -1;
        for(int i = 1; i < MAX_COMP_PAR; i++){
            double latency = estimateJoinLatency(i, leftParentParams, rightParentParams);
            if(latency < minLatency){
                minLatency = latency;
                parallelism = i;
            }
        }
        updateLatencies(parallelism, params, leftParentParams, rightParentParams);
        return parallelism;
    }
    
    //at the moment of invoking this, parallelism is not yet put in costParams of the component
    private void updateLatencies(int parallelism, CostParams params, CostParams leftParentParams, CostParams rightParentParams) {
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
        params.setBatchSize(batchSize);
    }
    
    
    //OPERATORS
    @Override
    protected void setBatchSize(OperatorComponent operator, Map<String, CostParams> compCost) {
        //this should be only at the last level, so we don't set this one
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
        
        return (((double)parallelism + 1) / 2) * Constants.getSerTime(leftBatchIn);
    }

    private double estimateSndTimeRightParent(int parallelism, CostParams rightParentParams) {
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        
        return (((double)parallelism + 1) / 2) * Constants.getSerTime(rightBatchIn);
    }    
    
    private double estimateJoinRcvTime(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        int leftBatchSize = leftParentParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        int leftParallelism = leftParentParams.getParallelism();
        int rightParallelism = rightParentParams.getParallelism();
        
        return leftParallelism * Constants.getDeserTime(leftBatchIn) + 
                rightParallelism * Constants.getDeserTime(rightBatchIn);
        
    }
    
    private double estimateJoinUsefullLatency(int parallelism, CostParams leftParentParams, CostParams rightParentParams) {
        int leftBatchSize = leftParentParams.getBatchSize();
        int leftBatchIn = leftBatchSize / parallelism;
        int rightBatchSize = rightParentParams.getBatchSize();
        int rightBatchIn = rightBatchSize / parallelism;
        int leftParallelism = leftParentParams.getParallelism();
        int rightParallelism = rightParentParams.getParallelism();
        
        double iqs = leftParallelism * leftBatchIn + rightParallelism * rightBatchIn;
        return  Constants.getJoinTime() * iqs;
    }    
}