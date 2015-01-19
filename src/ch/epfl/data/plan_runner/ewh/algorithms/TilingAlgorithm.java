package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.List;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;

public interface TilingAlgorithm {

    // sb is for collecting stats
    public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb);

    // returns null for those algorithm which do not use it (e.g. Okcan)
    public WeightPrecomputation getPrecomputation();

    // is always specified; that's what algorithm's optimality will be tested
    // against
    // not necessarily what it opimizes for (e.g. Okcan)
    public WeightFunction getWeightFunction();

    public double getWeight(Region region);

    public String getShortName();

}
