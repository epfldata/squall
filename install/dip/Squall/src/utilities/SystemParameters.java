package utilities;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class SystemParameters{
    private static Logger LOG = Logger.getLogger(SystemParameters.class);

    //in Local Mode, we compare against a file in this dir, for example with the name "hyracks.result"
    public static final String RESULT_DIR = "../checkResults";

    //default port, should not be changed unless some other application took this port
    public static final int NIMBUS_THRIFT_PORT = 6627;

    //used only in clustered mode execution
    //max number of tuples sent before receiving ack for any of them
    public static final int MAX_SPOUT_PENDING=5000;

    //used in StormWrapper for submitting topology, for both local and clustered mode
    //local mode: TPCH7 1GB needs 3000sec, TPCH8 100MB needs 1000s
    //clustered mode: TPCH7 1GB needs 30sec, 2GB needs 60sec, 8GB needs 90sec, 10GB needs 150sec
    //                all TPCH3 are done with 150sec
    public static final int MESSAGE_TIMEOUT_SECS = 150;
    //used in StormDataSource, for both local and clustered mode
    public static final long EOF_TIMEOUT_MILLIS = 1000;
    // Period between figuring out code is finished and
    //   killing the execution
    // used for both local and clustered mode execution
    public static final long SLEEP_BEFORE_KILL_MILLIS = 2000;

    //DO NOT MODIFY OR MOVE ANYWHERE ELSE. THESE ARE NOT CONFIGURATION VARIABLES
    public static final String DATA_STREAM = Utils.DEFAULT_STREAM_ID; /* "default" */
    public static final String EOF_STREAM = "2";
    public static final String DUMP_RESULTS_STREAM = "3";

    public static final String LAST_ACK = "LAST_ACK";
    public static final String EOF = "EOF";
    public static final String DUMP_RESULTS = "DumpResults";
    
    public static String getString(Map conf, String key){
        String result =  (String) conf.get(key);
        if (result == null || result.equals(""))
            LOG.info ("null in getString method for key " + key);
        return result;
    }

    public static int getInt(Map conf, String key){
        String result = getString(conf, key);
        return Integer.parseInt(result);
    }

    public static double getDouble(Map conf, String key){
        String result = getString(conf, key);
        return Double.parseDouble(result);
    }

     public static boolean getBoolean(Map conf, String key) {
        String result = getString(conf, key);
        if(result.equalsIgnoreCase("TRUE")){
            return true;
        }else if(result.equalsIgnoreCase("FALSE")){
            return false;
        }else{
            throw new RuntimeException("Invalid Boolean value");
        }
    }

    public static void putInMap(Map conf, String key, String value){
        conf.put(key, value);
    }

    public static void putInMap(Map conf, String key, Object value){
        putInMap(conf, key, String.valueOf(value));
    }

    public static Config fileToStormConfig(String propertiesFile) {
        Map map = fileToMap(propertiesFile);
        return mapToStormConfig(map);
    }

    public static Config mapToStormConfig(Map map){
        Config conf = new Config();
        conf.putAll(map);
        setStormVariables(conf);
        return conf;
    }

    public static Map<String, String> fileToMap(String propertiesFile){
        Map map = new HashMap<String, String>();

        try {
            String line="";
            BufferedReader reader = new BufferedReader(new FileReader(new File(propertiesFile)));
            while( (line = reader.readLine()) != null){

                // remove leading and trailing whitespaces
                line = line.trim();
            	if (line.length()!=0 && line.charAt(0)!='\n' //empty line
                        && line.charAt(0)!='\r' //empty line
                        && line.charAt(0)!='#'){  //commented line

                    //get key and value as first and second parameter
                    //delimiter is one or more whitespace characters (space, tabs, ...)
                    String args[] = line.split("\\s+");
                    if(args.length == 2){
                        //only in that case you put something in map
                        String key = args[0];
                        String value = args[1];
                        map.put(key, value);
                    }
                }
            }
            reader.close();
	} catch (Exception e) {
            String error=MyUtilities.getStackTrace(e);
            LOG.info(error);
            throw new RuntimeException(error);
	}
        return map;
    }

    /* Decided not to set it here, because we have to change many Squall config files
     *   this way the only change is in storm.yaml.
     *
     * Set variables for which there is no set method in Config
     * This overrides ~/.storm/storm.yaml
     * Thus, we don't need to change storm.yaml
     *   when submission to other master is required.
     * Still, these properties stay in storm.yaml,
     *   so when storm.yaml is uploaded,
     *   all the worker nodes are aware of the appropriate master node.
     *   This is important for multiple-instance Storm on cluster.
     */
    private static void setStormVariables(Config conf) {
//        if(SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED")){
//            String nimbusHost = SystemParameters.getString(conf, "DIP_NIMBUS_HOST");
//            conf.put(Config.NIMBUS_HOST, nimbusHost);
//            String stormZookeeperServers = SystemParameters.getString(conf, "DIP_STORM_ZOOKEEPER_SERVERS");
//            conf.put(Config.STORM_ZOOKEEPER_SERVERS, stormZookeeperServers);
//        }
    }
}
