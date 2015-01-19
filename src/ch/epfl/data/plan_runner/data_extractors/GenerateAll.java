package ch.epfl.data.plan_runner.data_extractors;

public class GenerateAll {

    public static void main(String[] args) throws Exception {
	ResultsGenerator.main(args); // creates Results.csv and Memory.csv
	GenerateThroughput.main(args); // both for non-scalability and
				       // scalability
	GenerateThroughputBars.main(args); // both for non-scalability and
					   // scalability
	GenerateMemory.main(args); // only for non-scalability
	GenerateMemoryBars.main(args); // only for non-scalability

	WeakScalabilityTimeGenerate.main(args);
	ExecutionTimeGenerate.main(args);

	GenerateOperatorLatency.main(args);

    }

}
