import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame

/**
  * Object to transform data for prediction models
  */
object ModelData {

  //Array of feature for ml models
  val features = Array("AdSlotWidth","AdSlotHeight","AdSlotVisibility",
    "AdSlotFormat","CreativeID","City","Region","Hour","TotalAdViews","TotalImpressions")

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
    * Pass multiple df columns for encoding
    *
    * @param df
    * @return
    */
  def multiBinaryFeatures(df:DataFrame):DataFrame = {
    features.foldLeft(df) {
      case (df, col) => singleBinaryFeature(df, col)
    }
  }

  /**
    * Helper Method to make a vector Assembler
    *
    * @param df
    * @param target
    * @return
    */
  def makeVectorAssembler(df: DataFrame,target: Array[String] ): VectorAssembler ={

    val assembler = new VectorAssembler().setInputCols(target.map(makeVectorColumn)).setOutputCol("features")

    assembler

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
