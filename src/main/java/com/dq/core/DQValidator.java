package com.dq.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.UserDefinedFunction;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.dmg.pmml.DataType;
import org.apache.spark.sql.functions;

import scala.Tuple2;

import com.dq.constraints.ColumnConstraint;
import com.dq.constraints.NeverNullColumn;
import com.dq.constraints.NonUniqueKeyColumn;
import com.dq.constraints.UniqueKeyColumn;
import com.dq.mappings.Column;
import com.dq.mappings.Table;
import com.dq.utils.FAEError;
import com.dq.utils.FAEUtils;
import com.esotericsoftware.minlog.Log;

import org.apache.spark.sql.api.java.UDF1;

public class DQValidator implements Serializable {

	private static Logger LOGGER = Logger.getLogger(DQValidator.class);
	private Arrays fastArray;

	public DQValidator() {

	}

	public void validate(String aSrcPath, String aDestPath,
			String aDestFileName, int colCount, final String aDelimiter,
			Column[] aColArr) {
		validateDelimitedColumn(colCount, aSrcPath, aDelimiter, "./DQ/DQ-"
				+ aDestFileName);
		validateAll(aSrcPath, aDestPath, aDestFileName, colCount, aDelimiter,
				aColArr);
	}

	protected void validateDelimitedColumn(int aColCount, String aSrcPath,
			String aDelimiter, String aDestPath) {
		JavaRDD<DataCheck> colDataCheck = DQSparkUtil.createDataCheckRDD(
				"PreLoad-01", true, aSrcPath, aDelimiter);
		JavaPairRDD<Boolean, DataCheck> rddColDataCheck = colDataCheck
				.mapToPair(new ColumnConstraint(aColCount));
		DQSparkUtil.writeToHadoopByBoolean(rddColDataCheck, aDestPath);
	}

	public Map<String, List<DQValidations>> getDQColumnMap(Column[] aColArr) {
		Map<String, List<DQValidations>> colValidMap = new HashMap<String, List<DQValidations>>();
		for (Column col : aColArr) {
			List<String> lstStrValid = col.getValidation();
			List<DQValidations> lstDQValidations = new ArrayList(
					lstStrValid.size());
			for (String lStrValid : lstStrValid) {
				lstDQValidations.add(DQValidations.valueOf(lStrValid));

			}
			DQColumn dqCol = new DQColumn(col.getName(), col.getDataType(),
					lstDQValidations);
			colValidMap.put(dqCol.getName(), dqCol.getColumnConstraint());
		}
		return colValidMap;
	}

	protected void validateAll(String aSrcPath, String aDestPath,
			String aDestFileName, int colCount, final String aDelimiter,
			Column[] aColArr) {
		StructField[] schemaFld = DQSparkUtil.extractFieldsFromString(aColArr);
		StructField[] newSchemaFld = this.addDescriptionAndFlag(schemaFld);
		final int oldSize = schemaFld.length;
		final int newSize = newSchemaFld.length;
		JavaRDD<String> rddFileStr = DQSparkUtil.createRowRDDString(
				"PreLoad-02", true, "./DQ/DQ-" + aDestFileName + "/dq-pass");

		JavaRDD<Row> rowData = rddFileStr.map(new Function<String, String[]>() {
			@Override
			public String[] call(String line) throws Exception {
				return line.split("\t");
			}
		}).map(new Function<String[], Row>() {
			@Override
			public Row call(String[] r) throws Exception {
				Object[] newRow = new Object[newSize];
				int rowSize = r.length;
				for (int i = 0; i < rowSize; i++) {
					if (r[i] != null) {
						newRow[i] = r[i];
					}
				}
				newRow[rowSize] = "";
				newRow[rowSize + 1] = "";
				newRow[rowSize + 2] = false;
				return RowFactory.create(newRow);
			}
		});

		SQLContext sqlContext = DQSparkUtil.createSparkSQLContext("PreLoad-02",
				true);
		StructType sparkSchema = new StructType(newSchemaFld);
		System.out.println("SchemaSize ::" + sparkSchema.defaultSize());
		DataFrame dfOriginal = sqlContext.createDataFrame(rowData, sparkSchema);
		// DataFrame dfWithId=dfOriginal.withColumn("Id",
		// org.apache.spark.sql.functions.monotonicallyIncreasingId());

		// dfOriginal.toJavaRDD().zipWithIndex() .map(new Function<Tuple2<Row,
		// Long>, Row>() {
		//
		// @Override public Row call(Tuple2<Row, Long> v1) throws Exception {
		// return RowFactory.create(v1._1().getString(0), v1._2()); } });
		//

		Map<String, List<DQValidations>> mpColValid = getDQColumnMap(aColArr);
		Iterator<Map.Entry<String, List<DQValidations>>> itr1 = mpColValid
				.entrySet().iterator();
		while (itr1.hasNext()) {
			Map.Entry<String, List<DQValidations>> entry = itr1.next();
			String lColName = entry.getKey();
			List<DQValidations> lLstValid = entry.getValue();
			validateColumn(lColName, dfOriginal, lLstValid);
		}
		dfOriginal.show();
		// dfWithId.show();
	}

	public void validateColumn(String aColName, DataFrame aDataFrame,
			List<DQValidations> aLstValidations) {
		for (DQValidations lValidations : aLstValidations) {
			switch (lValidations) {
			case UNIQUE:
				checkUniqueKeyColumns(aDataFrame, aColName);
				checkNonUniqueKeyColumns(aDataFrame, aColName);
				break;
			case NOTNULL:
				checkNullColumns(aDataFrame, aColName);
				break;
			default:
				LOGGER.info("Not Supported Validations..");
			}

		}
	}

	private void checkNullColumns(DataFrame aDataFrame, String aColName) {
		NeverNullColumn lNeverNulllCol = new NeverNullColumn();
		DataFrame nullDataFrame = lNeverNulllCol.validate(aDataFrame, aColName);
		nullDataFrame.write().format("com.databricks.spark.csv")
				.mode(SaveMode.Overwrite).save("./tier-2/NonDistinct/DQ-NULL");
		// DQSparkUtil.writeToHadoopByBoolean(rddColDataCheck, aDestPath);
	}

	private void checkUniqueKeyColumns(DataFrame aDataFrame, String aColName) {
		UniqueKeyColumn lUniqCols = new UniqueKeyColumn();
		DataFrame lDataDistinct = lUniqCols.validate(aDataFrame, aColName);
		lDataDistinct.write().format("com.databricks.spark.csv")
				.mode(SaveMode.Overwrite).save("./tier-2/Distinct/DQ-UNIQUE");
	}

	private void checkNonUniqueKeyColumns(DataFrame aDataFrame, String aColName) {
		NonUniqueKeyColumn lNonUniqCols = new NonUniqueKeyColumn();
		DataFrame lDataNonnDistinct = lNonUniqCols.validate(aDataFrame,
				aColName);

		SQLContext sqlCtx = lDataNonnDistinct.sqlContext();

		sqlCtx.udf().register("errorDescription", new UDF1<String, String>() {
			@Override
			public String call(String str) throws Exception {
				return FAEError.DATA_FORMAT_ERROR.getErrorDescription();
			}
		}, DataTypes.StringType);

		DataFrame newDataNonDistinct = lDataNonnDistinct.withColumn("Error",
				org.apache.spark.sql.functions.callUDF("errorDescription",
						lDataNonnDistinct.col("Error")));
		newDataNonDistinct.show();
		newDataNonDistinct.write().format("com.databricks.spark.csv")
				.mode(SaveMode.Overwrite)
				.save("./tier-2/Distinct/DQ-NON-UNIQUE");
	}

	/*
	 * public void getValidationCheck(String aSourcePath, String aTargetPath,
	 * String aDelimit, Column[] arrCol) {
	 * 
	 * SparkConf conf = new SparkConf().setAppName("Schema")
	 * .setMaster("local"); int columnCount = arrCol.length; JavaSparkContext sc
	 * = new JavaSparkContext(conf);
	 * 
	 * // JavaRDD<String> emp = sc.textFile(aSourcePath); SQLContext sqlContext
	 * = new SQLContext(sc); JavaRDD<String> emp = sc.textFile(aSourcePath);
	 * StructField[] schemaFld = this.extractFieldsFromString(arrCol);
	 * 
	 * JavaRDD<Row> rowData = emp.map(new Function<String, String[]>() {
	 * 
	 * @Override public String[] call(String line) throws Exception {
	 * System.out.println(line); return line.split("\t"); } }).map(new
	 * Function<String[], Row>() {
	 * 
	 * @Override public Row call(String[] r) throws Exception { return
	 * RowFactory.create(r[1], r[2], r[3]); } }); StructType sparkSchema = new
	 * StructType(schemaFld); DataFrame df = sqlContext.createDataFrame(rowData,
	 * sparkSchema);
	 * 
	 * df.printSchema(); df.show(); // df.toDF(new String[]{"patId"}); DataFrame
	 * dfDistinct = df.dropDuplicates(new String[] { "patId" });
	 * 
	 * // dfDistinct.save(aTargetPath); dfDistinct.select("patId", "name",
	 * "age").write() .format("com.databricks.spark.csv").option("header",
	 * "false") .save(aTargetPath);
	 * 
	 * // DataFrame dfWhere=df.where(condition);
	 * 
	 * DataFrame dfDiff = UniqueKeyColumn.validate(df, "patId"); System.out
	 * .println("=========================================================");
	 * dfDiff.show(); //
	 * dfDiff.select("patId","name","age").write().mode(SaveMode
	 * .Append).format("com.databricks.spark.text").option("header", //
	 * "false").save("./tier-1/NonDistinct/"+aTargetPath);
	 * dfDiff.select("patId", "name", "age").write()
	 * .format("com.databricks.spark.csv").mode(SaveMode.Overwrite)
	 * .save("./tier-1/NonDistinct/" + aTargetPath);
	 * 
	 * NeverNullColumn.validate(df, "name"); // dfDistinct.saveAsTable("tier");
	 * 
	 * UniqueKeyColumn uniqKyCol=new UniqueKeyColumn();
	 * uniqKyCol.validate(df,"patId");
	 * 
	 * 
	 * // DataFrame df=sqlContext.createDataFrame(rowData, sparkSchema);
	 * 
	 * }
	 */
	private StructField[] addDescriptionAndFlag(StructField[] aStructFld) {
		int totFlds = (aStructFld.length) + 3;
		StructField[] aNewStructFields = new StructField[(aStructFld.length) + 3];
		for (int i = 0; i < aStructFld.length; i++) {
			aNewStructFields[i] = aStructFld[i];

		}
		StructField fieldId = new StructField("Id", DataTypes.StringType, true,
				Metadata.empty());
		StructField fieldErr = new StructField("Error", DataTypes.StringType,
				true, Metadata.empty());
		StructField fieldFlg = new StructField("isBad", DataTypes.BooleanType,
				true, Metadata.empty());
		aNewStructFields[totFlds - 1] = fieldFlg;
		aNewStructFields[totFlds - 2] = fieldErr;
		aNewStructFields[totFlds - 3] = fieldId;
		return aNewStructFields;
	}

}
