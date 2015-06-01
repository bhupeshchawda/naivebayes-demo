package com.datatorrent.demos.ml.classifier.nb;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.ml.classification.NBConfig;
import com.datatorrent.lib.ml.classification.NBInputReader;
import com.datatorrent.lib.ml.classification.FileInputOperator;
import com.datatorrent.lib.ml.classification.NBOutputPerWindowOperator;
import com.datatorrent.lib.ml.classification.NBCounter;
import com.datatorrent.lib.ml.classification.NBModelAggregator;
import com.datatorrent.lib.ml.classification.NBModelStorage;

public class Application implements StreamingApplication {

	public void populateDAG(DAG dag, Configuration conf) {

		NBConfig nbc = new NBConfig(true, 3);
		
		// File Input Operator
		FileInputOperator opInput = dag.addOperator("FileInput", new FileInputOperator());
		opInput.setDirectory("/input");

		// Input Reader
		NBInputReader opNBInputReader = dag.addOperator("NBInput", new NBInputReader(nbc));
		
		// NB Counter
		NBCounter opNBCounter = dag.addOperator("NBCounter", new NBCounter(nbc));
		
		// NB Aggregator
		NBModelAggregator<NBModelStorage> opNBAggregator = 
				dag.addOperator("NBAggregator", new NBModelAggregator<NBModelStorage>(nbc));
		
		// File Output Operator
		NBOutputPerWindowOperator opNBOutput = dag.addOperator("NBModelWriter", new NBOutputPerWindowOperator(nbc));
		opNBOutput.setFilePath("/pmmloutput");
		opNBOutput.setFileName("PMML_HIGGS_CATEGORICAL.xml");
		opNBOutput.setOverwrite(true);

		// Streams
		
		dag.addStream("FileInput To NBInput", opInput.output, opNBInputReader.input).setLocality(Locality.THREAD_LOCAL);
		
		//Training FLow
		dag.addStream("NBInput To NBCounter", opNBInputReader.output, opNBCounter.input);
		dag.addStream("NBCounter to NBAggregator", opNBCounter.output, opNBAggregator.data);
		dag.addStream("NBAggregator To NBModelWriter", opNBAggregator.output, opNBOutput.input);

		//K Fold Validation Flow
		dag.addStream("NBInput - NBCounter", opNBInputReader.kFoldOutput, opNBCounter.kFoldInput);
		dag.addStream("NBCounter - NBAggregator", opNBCounter.kFoldOutput, opNBAggregator.kFoldInput);
		dag.addStream("NBAggregator - NBModelWriter", opNBAggregator.kFoldOutput, opNBOutput.kFoldInput);
		
	}
}
