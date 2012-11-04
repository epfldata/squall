
package sql.optimizers.name.manual_batching;


public class ClusterConstants {

    public static double getJoinTime() {
        return 0.0037;
    }

    public static double getReadTime() {
        return 0.0015;
    }
    
    //TODO: for now only in tpch4, we are doing aggregate
    public static double getOpTime() {
        return getReadTime(); // there is one access to memory, so it can be approximated by this
    }    
    
    public static double getSerTime(int batchSize) {
        if(batchSize <= 16){
            return 0.15;
        }else if(batchSize <= 32){
            return 0.20;
        }else if(batchSize <= 64){
            return 0.30;
        }else if(batchSize <= 128){
            return 0.40;
        }else if(batchSize <= 256){
            return 0.58;
        }else if(batchSize <= 512){
            return 0.59;
        }else if(batchSize <= 1024){
            return 0.67;
        }else if(batchSize <= 2048){
            return 0.80;
        }else if(batchSize <= 4096){
            return 0.95;
        }else{
            throw new RuntimeException("Missing measurements results for deserialization for bs = " + batchSize + ".");
        }
    }
    
    public static double getDeserTime(int batchSize) {
        if(batchSize <= 16){
            return 0.45;
        }else if(batchSize <= 32){
            return 0.55;
        }else if(batchSize <=64){
            return 0.70;
        }else if(batchSize <= 128){
            return 0.90;
        }else if(batchSize <= 256){
            return 1.04;
        }else if(batchSize <= 512){
            return 1.59;
        }else if(batchSize <= 1024){
            return 2.49;
        }else if(batchSize <= 2048){
            return 5.00;
        }else if(batchSize <= 4096){
            return 7.50;
        }else{
            throw new RuntimeException("Missing measurements results for serialization for bs = " + batchSize + ".");
        }
    }
        
}