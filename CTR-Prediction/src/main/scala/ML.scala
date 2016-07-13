import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object ML {

  def main(args: Array[String]) {

    //spark engine config
    val conf = new SparkConf().setAppName("ctr-prediction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    run(sc, sqlContext)
  }

  def run(sc: SparkContext, sqlContext: SQLContext) {
    val df = Data.getSingleFrame(sc, sqlContext)
    singleFeature(castTypes(df))
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
    * Applying logistic regression algorithm using a single feature
    *
    * @param df
    */
  def singleFeature(df: DataFrame){
    val weights = Array(0.8, 0.2)
    val seed = 11L

    //mapping string columns to indices
    val indexer = new StringIndexer().setInputCol("AdSlotFormat").setOutputCol("AdSlotFormat-Index")
    val indexed = indexer.fit(df).transform(df)
    //    indexed.show()

    //converting a categorical feature to a binary vector
    val encoder = new OneHotEncoder().setInputCol("AdSlotFormat-Index").setOutputCol("AdSlotFormat-Vector")
    val encoded = encoder.transform(indexed)
    //    encoded.select("Click", "AdSlotFormat-Vector").show()

    //creating label points from data-frame
    val labeledData = encoded.map{
      row =>
        LabeledPoint(row.getAs[Double]("Click"),row.getAs[Vector]("AdSlotFormat-Vector"))
    }

    //splitting data-set into train and test sets
    val Array(trainingSet, testingSet) = labeledData.randomSplit(weights, seed)
    trainingSet.cache()

    //LR training algorithm
    val lrModel = new LogisticRegressionWithLBFGS().run(trainingSet)

    //clears threshold - so model can return probabilities
    lrModel.clearThreshold()
  }

}
