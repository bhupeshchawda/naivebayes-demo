package com.datatorrent.demos.ml.classifier.nb;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.ml.classification.ARFFReader;
import com.datatorrent.lib.ml.classification.FileAppendOutputOperator;
import com.datatorrent.lib.ml.classification.FileInputOperator;
import com.datatorrent.lib.ml.classification.NaiveBayesEvaluator;

public class ApplicationEvaluate implements StreamingApplication {

	public void populateDAG(DAG dag, Configuration conf) {

		FileInputOperator opInput = dag.addOperator("Input Operator", new FileInputOperator());
		opInput.setDirectory("/test");

		ARFFReader opArffReader = dag.addOperator("ARRFReader", new ARFFReader());
		
		NaiveBayesEvaluator opEval = dag.addOperator("NaiveBayesEvaluator", new NaiveBayesEvaluator());
		opEval.setPmmlFilePath("/pmmloutput/PMML_HIGGS_CATEGORICAL.xml");
		
		FileAppendOutputOperator opOutput = dag.addOperator("OutputWriter", new FileAppendOutputOperator());
		opOutput.setFilePath("/testOutput");
		opOutput.setFileName("output");
		
		dag.addStream("File Reader To Arff Reader", opInput.output, opArffReader.input);
		dag.addStream("Arff Reader To NB Evaluator", opArffReader.output, opEval.input);
		dag.addStream("NB Eval to Writer", opEval.output, opOutput.input);
	}
}
