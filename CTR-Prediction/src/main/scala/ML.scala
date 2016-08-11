import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Object to create prediction models
  *
  * @author Ehshan-Veerabangsa
  */
object ML {

  /**
    * Array of creative features to target
    */
  val creativeTarget = Array("AdSlotWidth","AdSlotHeight","AdSlotVisibility",
    "AdSlotFormat","CreativeID")


  /**
    * Applying logistic regression model - using a single feature
    *
    * @param df
    */
  def lrModelSingle(df: DataFrame){

    //mapping string columns to indices
    val indexer = new StringIndexer().setInputCol("AdSlotFormat").setOutputCol("AdSlotFormat-Index")
    val indexed = indexer.fit(df).transform(df)

    //converting a categorical feature to a binary vector
    val encoder = new OneHotEncoder().setInputCol("AdSlotFormat-Index").setOutputCol("AdSlotFormat-Vector")
    val encoded = encoder.transform(indexed)

    //creating label points from data-frame
    val labeledData = makeLabelPoints(encoded,"AdSlotFormat-Vector")

    //logistic regression training and testing algorithm
    runLr(labeledData)
  }

  /**
    * Applying logistic regression model to a vector of creative features
    *
    * @param df
    */
  def lrModel(df: DataFrame){

    val labeledData = makeLabelPoints(df, "features")

    runLr(labeledData)
  }

  /**
    * Logistic Regression training + test Algorithm
    *
    * @param rdd
    */
  def runLr(rdd: RDD[LabeledPoint]){
    val weights = Array(0.8, 0.2)
    val seed = 11L

    //splitting data-set into train and test sets
    val Array(trainingSet, testingSet) = rdd.randomSplit(weights, seed)
    trainingSet.cache()

    //LR training algorithm
    val lrModel = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingSet)

    //clears threshold - so model can return probabilities
    lrModel.clearThreshold()

    //predicting click on training set using results from LR algorithm
    val predictions = testingSet.map{
      case LabeledPoint(label, features) =>
        val prediction = lrModel.predict(features)
        (prediction, label)
    }

    // creating metric object for evaluation
    val bcMetric = new BinaryClassificationMetrics(predictions)

    Store.saveModelResults(bcMetric)
  }

  /**
    * Logistic Regression model using pipeline for parameter optimisation
    *
    * @param df
    */
  def lrModelTuned(df:DataFrame){

    val clean = ModelData.dropNonFeatures(df)

    val encodedData = ModelData.multiBinaryFeatures(clean)

    val (trainingSet, testingSet) = splitData(encodedData)

    val va = ModelData.makeVectorAssembler(encodedData,ModelData.features)

    val lr = new LogisticRegression().setLabelCol("Click")

    val pipeline = new Pipeline().setStages(Array(va, lr))

    val paramMap = makeParamGrid(lr)

    val crossVal = makeCrossValidator(pipeline, paramMap)

    val cvModel = crossVal.fit(trainingSet)

    //EVALUATING TEST SET ON MODEL
    val cvTransformed = cvModel.transform(testingSet)

    val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("Click")
      .setRawPredictionCol("rawPrediction")

    //GET METRIC VALUES
    def printlnMetric(metricName: String): String = {
      metricName + " = " + binaryClassificationEvaluator.setMetricName(metricName).evaluate(cvTransformed)
    }
    println("Area Under Curve: "+printlnMetric("areaUnderROC"))
    println("Area Under Precision Recall: "+printlnMetric("areaUnderPR"))

    //PROBABILITY
    val cvProbability = cvTransformed.select("label","probability")

    val probDf = splitProbability(cvProbability)

    //RESULTS OF GRID SEARCH
    cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).foreach(println)

    //BEST PARAMETERS
    println("Best set of parameters found:" + cvModel.getEstimatorParamMaps
      .zip(cvModel.avgMetrics)
      .maxBy(_._2)
      ._1)
  }

  /**
    * Helper method to cast click column from a string to a double
    *
    * @param df
    * @return
    */
  def castTypes(df: DataFrame): DataFrame={
    val castDf = df
      .withColumn("clickTmp", df("Click").cast(DoubleType))
      .drop("Click")
      .withColumnRenamed("clickTmp", "Click")
    castDf
  }

  /**
    * Method to split Data-frame into training and testing set
    *
    * @param df
    * @return
    */
  def splitData(df: DataFrame):(DataFrame,DataFrame)={
    val weights = Array(0.8, 0.2)
    val seed = 11L
    val split = df.randomSplit(weights, seed)
    (split(0),split(1))
  }

  /**
    * Helper Method to create label point RDD from Data-frame
    *
    * @param df
    * @return
    */
  def makeLabelPoints(df:DataFrame,col: String):RDD[LabeledPoint] ={
    df.map{
      row => LabeledPoint(row.getAs[Double]("Click"),row.getAs[Vector](col))
    }
  }

  /**
    * Making a Parameter Map
    *
    * @param lr
    * @return
    */
  def makeParamGrid(lr: LogisticRegression): Array[ParamMap] ={
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01,0.1,1.0))
      .addGrid(lr.elasticNetParam, Array(0.0,0.8,1.0))
      .addGrid(lr.fitIntercept, Array(x = false))
      //.addGrid(lr.maxIter, Array(10,20))
      .build()
    paramGrid
  }

  /**
    *
    * @param pipeline
    * @param paramGrid
    * @return
    */
  def makeCrossValidator(pipeline: Pipeline, paramGrid: Array[ParamMap]): CrossValidator ={
    val crossVal = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Click"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
    crossVal
  }

  /**
    * Method to split probability vector from prediction to click and no-click columns
    *
    * @param df
    * @return
    */
  def splitProbability(df: DataFrame): DataFrame ={
    //SPLITS THE PROBABILITY TO CLICK/NO-CLICK PROBABILITIES
    val no: (Vector => (Double)) = (arg: Vector) => arg(0)
    val yes:(Vector => (Double)) = (arg: Vector) => arg(1)

    val noClickProb = udf(no)
    val clickProb = udf(yes)

    df.withColumn("No-Click-Probability", noClickProb(df("Probability")))
      .withColumn("Month", clickProb(df("Probability")))
  }


}
