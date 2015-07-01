package com.datatorrent.demos.ml.classifier.nb;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.ml.classification.NBConfig;
import com.datatorrent.lib.ml.classification.NBEvaluator;
import com.datatorrent.lib.ml.classification.NBInputReader;
import com.datatorrent.lib.ml.classification.NBLineReader;
import com.datatorrent.lib.ml.classification.NBOutputPerWindowOperator;
import com.datatorrent.lib.ml.classification.NBCounter;
import com.datatorrent.lib.ml.classification.NBModelAggregator;
import com.datatorrent.lib.ml.classification.NBModelStorage;

/**
 * Application for training the Naive Bayes Model.
 * This application has two flows - 
 * 1. Training flow
 * 2. K-fold Cross Evaluation flow
 * 
 * The NBConfig object which is passed to operators configures the operators for the different flows.
 * 
 * @author bhupesh
 *
 */
@ApplicationAnnotation(name="NaiveBayes-Demo")
public class NaiveBayesDemo implements StreamingApplication {

  private boolean isKFold;
  private boolean isTrain;
  private boolean isEvaluate;
  private int numFolds;
  private int numAttributes;
  private int numClasses;
  private String inputDataFilePath;
  private String modelDir;
  private String modelFileName;
  private String resultDir;
  private String resultFileName;
  
  
  public NaiveBayesDemo(){

  }
  
  public void populateDAG(DAG dag, Configuration conf) {

    /*
     * Config for k-fold cross validation of the Naive Bayes Model
     */

    numFolds = Integer.parseInt(conf.get("dt.ml.classification.nb.numFolds"));
    numAttributes = Integer.parseInt(conf.get("dt.ml.classification.nb.numAttributes"));
    numClasses = Integer.parseInt(conf.get("dt.ml.classification.nb.numClasses"));
    inputDataFilePath = conf.get("dt.ml.classification.nb.inputDataFilePath");
    modelDir = conf.get("dt.ml.classification.nb.modelDir");
    modelFileName = conf.get("dt.ml.classification.nb.modelFileName");
    resultDir = conf.get("dt.ml.classification.nb.resultDir");
    resultFileName = conf.get("dt.ml.classification.nb.resultFileName");
    isKFold = conf.getBoolean("dt.ml.classification.nb.isKFold", false);
    isTrain = conf.getBoolean("dt.ml.classification.nb.isTrain", false);
    isEvaluate = conf.getBoolean("dt.ml.classification.nb.isEvaluate", false);

    System.out.println("NumFolds = "+numFolds);
    System.out.println("isKFold = "+isKFold);
    System.out.println("isTrain = "+isTrain);
    System.out.println("isEvaluate = "+isEvaluate);
    System.out.println("Input file path = "+inputDataFilePath);
    
    if(!isKFold && !isTrain && !isEvaluate){
      System.out.println("Invalid Params. K-Fold evaluation, Training or Testing must be selected");
      return;
    }
    if(isKFold && numFolds <= 1){
      System.out.println("Invalid Params. Number of folds should be  > 1");
      return;
    }
    
    NBConfig nbc = new NBConfig(
        isKFold,              // K-Fold Validation 
        isTrain,               // Only Train
        isEvaluate,              // Only Evaluate
        numFolds,                 // Number of folds
        numAttributes,                 //Number of attributes
        numClasses,                  //Number of classes
        inputDataFilePath,       // Input Data File
        modelDir,          // Model Dir
        modelFileName,  // Model File Name Base
        resultDir,           // Result Dir
        resultFileName            // Result file name
        );

//    NBConfig nbc = new NBConfig(
//        true,              // K-Fold Validation 
//        true,               // Only Train
//        false,              // Only Evaluate
//        5,                 // Number of folds
//        28,                 //Number of attributes
//        2,                  //Number of classes
//        "/input/HIGGS_TRAIN",       // Input Data File
//        "/pmmloutput",          // Model Dir
//        "PMML_HIGGS_CATEGORICAL.xml",  // Model File Name Base
//        "/testOutput",           // Result Dir
//        "output"            // Result file name
//        );
    
    /*
     * Define Operators
     */
    
    // File Input Operator
    NBLineReader opInput = dag.addOperator("File_Input", new NBLineReader(nbc));

    // Input Reader
    NBInputReader opNBInputReader = dag.addOperator("Parser", new NBInputReader(nbc));
    
    // NB Counter
    NBCounter opNBCounter = dag.addOperator("Counter", new NBCounter(nbc));
    
    // NB Aggregator
    NBModelAggregator<NBModelStorage> opNBAggregator = 
        dag.addOperator("Model_Updater", new NBModelAggregator<NBModelStorage>(nbc));
    
    //NB Evaluator
    NBEvaluator opNBEvaluator = dag.addOperator("Evaluator", new NBEvaluator(nbc));        
    
    // File Output Operator
    NBOutputPerWindowOperator opNBOutput = dag.addOperator("Model_Writer", new NBOutputPerWindowOperator(nbc));

    /*
     * Define Streams
     */
    dag.addStream("To Parser", opInput.lineOutputPort, opNBInputReader.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("Control_Parser", opInput.controlOut, opNBInputReader.controlIn).setLocality(Locality.THREAD_LOCAL);
    
    dag.addStream("To Counter", opNBInputReader.outForTraining, opNBCounter.inTraining);
    dag.addStream("To Model Updater", opNBCounter.outTraining, opNBAggregator.inTraining);
    dag.addStream("To Model Writer", opNBAggregator.outTraining, opNBOutput.inMultiWriter);

    dag.addStream("To Evaluator", opNBInputReader.outForEvaluation, opNBEvaluator.inForEvaluation);
    dag.addStream("To Result Writer", opNBEvaluator.outToWriter, opNBOutput.inStringWriter);

    dag.addStream("Control_Counter", opNBInputReader.controlOut, opNBCounter.controlIn);
    dag.addStream("Control_Updater", opNBCounter.controlOut, opNBAggregator.controlIn);
    dag.addStream("Control_Writer", opNBAggregator.controlOut, opNBOutput.controlIn);
    
    dag.addStream("To Evaluator (K-fold models)", opNBAggregator.outEvaluator, opNBEvaluator.inKFoldModels);
    
    
  }
}
