import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame

/**
  * Object to transform data for prediction models
  */
object ModelData {

  /**
    * Method to encode a single passed to column of labeled indices & vector column of indices
    *
    * @param df
    * @param column
    * @return
    */
  def singleBinaryFeature(df: DataFrame,column: String): DataFrame = {
    val labelIndexer = new StringIndexer()
      .setInputCol(column)
      .setOutputCol(makeIndexColumn(column))
      .fit(df)
      .transform(df)


    val encoder = new OneHotEncoder()
      .setDropLast(false)
      .setInputCol(makeIndexColumn(column))
      .setOutputCol(makeVectorColumn(column))

    encoder.transform(labelIndexer)
      .drop(column)
      .drop(makeIndexColumn(column))
  }

  /**
    * Makes a index column for string-indexer
    *
    * @param col
    * @return
    */
  def makeIndexColumn(col: String) = col + "-index"

  /**
    * Makes a vector column for one-hot-encoder
    *
    * @param col
    * @return
    */
  def makeVectorColumn(col: String) = col + "-vector"

}
