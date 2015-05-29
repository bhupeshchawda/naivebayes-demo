package com.datatorrent.demos.ml.classifier.nb;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.ml.classification.ARFFReader;
import com.datatorrent.lib.ml.classification.FileInputOperator;
import com.datatorrent.lib.ml.classification.FileOutputPerWindowOperator;
import com.datatorrent.lib.ml.classification.NaiveBayesCounter;
import com.datatorrent.lib.ml.classification.NaiveBayesModelAggregator;
import com.datatorrent.lib.ml.classification.NaiveBayesModelStorage;

public class Application implements StreamingApplication {

	public void populateDAG(DAG dag, Configuration conf) {

		FileInputOperator opInput = dag.addOperator("Input Operator", new FileInputOperator());
		opInput.setDirectory("/input");

		ARFFReader opArffReader = dag.addOperator("ARRFReader", new ARFFReader());
		
		NaiveBayesCounter opNaiveBayesCounter = dag.addOperator("Naive Bayes Counter", new NaiveBayesCounter());
		
		NaiveBayesModelAggregator<NaiveBayesModelStorage> opNaiveBayesAggregator = 
				dag.addOperator("Naive Bayes Model Aggregator", new NaiveBayesModelAggregator<NaiveBayesModelStorage>());
		
		FileOutputPerWindowOperator opOutput = dag.addOperator("PMML Writer", new FileOutputPerWindowOperator());
		opOutput.setFilePath("/pmmloutput");
		opOutput.setFileName("PMML_HIGGS_CATEGORICAL.xml");
		opOutput.setOverwrite(true);
		
		dag.addStream("File Reader To Arff Reader", opInput.output, opArffReader.input).setLocality(Locality.THREAD_LOCAL);
		dag.addStream("Arff Reader To NB Counter", opArffReader.output, opNaiveBayesCounter.input);
		dag.addStream("NB Counter to NB Aggregator", opNaiveBayesCounter.output, opNaiveBayesAggregator.data);
		dag.addStream("NB Aggregator To Pmml Writer", opNaiveBayesAggregator.output, opOutput.input);
	}
}
