package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.FeederSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/*
 * This is a modified version of the code from storm-starter/src/jvm/trident.
 *    Very weird behavior.
 */
public class TridentUpdater {
	private static Logger LOG = Logger.getLogger(TridentUpdater.class);
	
    private static FeederSpout _spout;
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        _spout = new FeederSpout(new Fields("sentence"));
        
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
              topology.newStream("spout1", _spout)
                .parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))
                .parallelismHint(16);
        
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                //.aggregate(new Fields("count"), new Sum(), new Fields("sum"))
                ;
        
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for(int i=0; i<2; i++) {
                _spout.feed(new ArrayList(Arrays.asList("dog the the cat")));
                Thread.sleep(10000);
                LOG.info("DRPC RESULT: " + drpc.execute("words", "dog the dog cat"));
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}