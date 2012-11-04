
package sql.optimizers.name.manual_batching;


public class Constants {

    public static double getJoinTime() {
        return 0.0037;
    }

    public static double getReadTime() {
        return 0.0015;
    }    
    
    public static double getSerTime(int batchSize) {
        if(batchSize <= 128){
            return 0.40;
        }else if(batchSize <= 256){
            return 0.58;
        }else if(batchSize <= 512){
            return 0.59;
        }else if(batchSize <= 1024){
            return 0.67;
        }else{
            throw new RuntimeException("Missing measurements results for deserialization for bs = " + batchSize + ".");
        }
    }
    
    public static double getDeserTime(int batchSize) {
        if(batchSize <= 128){
            return 0.90;
        }else if(batchSize <= 256){
            return 1.04;
        }else if(batchSize <= 512){
            return 1.59;
        }else if(batchSize <= 1024){
            return 2.49;
        }else{
            throw new RuntimeException("Missing measurements results for serialization for bs = " + batchSize + ".");
        }
    }
        
}