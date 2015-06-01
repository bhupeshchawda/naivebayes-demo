package com.datatorrent.demos.ml.classifier.nb;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.ml.classification.NBConfig;
import com.datatorrent.lib.ml.classification.NBInputReader;
import com.datatorrent.lib.ml.classification.NBOutputAppendOperator;
import com.datatorrent.lib.ml.classification.FileInputOperator;
import com.datatorrent.lib.ml.classification.NBEvaluator;

public class ApplicationEvaluate implements StreamingApplication {

	public void populateDAG(DAG dag, Configuration conf) {
		
		NBConfig nbc = new NBConfig(true, 3); 
		
		FileInputOperator opInput = dag.addOperator("Input Operator", new FileInputOperator());
		opInput.setDirectory("/test");
		
		NBInputReader opArffReader = dag.addOperator("ARRFReader", new NBInputReader(nbc));
		
		//TODO Set nbc for other operators in evaliation flow when k fold evaluation is built into the operators
		
		NBEvaluator opEval = dag.addOperator("NaiveBayesEvaluator", new NBEvaluator());
		opEval.setPmmlFilePath("/pmmloutput/PMML_HIGGS_CATEGORICAL.xml");
		
		NBOutputAppendOperator opOutput = dag.addOperator("OutputWriter", new NBOutputAppendOperator());
		opOutput.setFilePath("/testOutput");
		opOutput.setFileName("output");
		
		dag.addStream("File Reader To Arff Reader", opInput.output, opArffReader.input);
		dag.addStream("Arff Reader To NB Evaluator", opArffReader.output, opEval.input);
		dag.addStream("NB Eval to Writer", opEval.output, opOutput.input);
	}
}
