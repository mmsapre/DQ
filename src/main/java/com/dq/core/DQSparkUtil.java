package com.dq.core;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import com.dq.mappings.Column;
import com.dq.utils.FAEUtils;

public final class DQSparkUtil {

	private static transient JavaSparkContext javaSparkCntxt = null;
	private static transient SQLContext sparkSQLCntxt = null;

	private DQSparkUtil() {

	}

	public static JavaSparkContext createSparkContext(String aAppName,
			boolean isLocal) {
		if (javaSparkCntxt == null) {
			SparkConf conf = new SparkConf().setAppName(aAppName);
			if (isLocal) {
				conf.setMaster("local");
			}
			javaSparkCntxt = new JavaSparkContext(conf);
		}
		return javaSparkCntxt;
	}

	public static SQLContext createSparkSQLContext(String aAppName,
			boolean isLocal) {
		if (sparkSQLCntxt == null) {
			SparkContext aSparkCntxt = createSparkContext(aAppName, isLocal)
					.sc();
			sparkSQLCntxt = new SQLContext(aSparkCntxt);
		}
		return sparkSQLCntxt;
	}

	public static JavaRDD<String> createRowRDDString(String aAppName,
			boolean isLocal, String aSrcPath) {
		if (javaSparkCntxt == null) {
			createSparkContext(aAppName, isLocal);
		}
		JavaRDD<String> rawDataRDD = javaSparkCntxt.textFile(aSrcPath);
		return rawDataRDD;
	}

	public static JavaRDD<DataCheck> createDataCheckRDD(String aAppName,
			boolean isLocal, String aSrcPath, final String aDelimiter) {
		JavaRDD<String> rawRDD = createRowRDDString(aAppName, isLocal, aSrcPath);
		JavaRDD<DataCheck> rawDataRDD = rawRDD
				.map(new Function<String, DataCheck>() {
					@Override
					public DataCheck call(String data) throws Exception {
						DataCheck dCheck = new DataCheck(data, aDelimiter);
						return dCheck;
					}

				});
		return rawDataRDD;
	}
	
	public  static void writeToHadoopByBoolean(JavaPairRDD<Boolean, DataCheck> aRddDataCheck,String aDestPath){
		aRddDataCheck.saveAsHadoopFile(aDestPath,NullWritable.class, String.class,RDDMultipleBooleanDataFormat.class);
	}

	
	public static StructField[] extractFieldsFromString(Column[] colArr) {
		StructField[] resFields = new StructField[(colArr.length)];
		String name, type;
		String[] strFieldTokens;
		for (int i = 0; i < colArr.length; i++) {

			name = colArr[i].getName().trim();
			type = colArr[i].getDataType().trim();
			
			StructField field = new StructField(name,
					FAEUtils.stringToDataType(type), true, Metadata.empty());
			resFields[i] = field;
		}
		return resFields;
	}
}
