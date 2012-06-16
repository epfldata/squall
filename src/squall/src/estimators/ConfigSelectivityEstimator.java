package estimators;

import java.util.Map;
import utilities.SystemParameters;


public class ConfigSelectivityEstimator{

    private Map _map;

    public ConfigSelectivityEstimator(Map map){
        _map = map;
    }

    /*
     * read selectivity from a config file
     */
    public double estimate(String compName) {
        String selStr = compName + "_SEL";
        return SystemParameters.getDouble(_map, selStr);
    }

}
