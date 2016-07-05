package com.dq.constraints;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import com.dq.core.DataCheck;
import com.dq.mappings.Column;
import com.dq.utils.FAEUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import scala.Tuple2;

class RDDMultipleTextRowFormat<Boolean> extends
		MultipleTextOutputFormat<Boolean, String> {

	@Override
	protected String generateFileNameForKeyValue(Boolean key, String value,
			String name) {
		if ("true".equalsIgnoreCase(key.toString()))
			return "TRUE";
		else
			return "FALSE";

	}
}

public class ColumnCheck implements Serializable{

	private static final Logger log = Logger.getLogger(ColumnCheck.class.getName());
	

	/*public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Schema")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// SQLContext sqlContext=new SQLContext(sc);
		JavaRDD<String> emp = sc.textFile("./employee.txt");

		JavaRDD<DataCheck> rowData = emp.map(new Function<String, DataCheck>() {
			@Override
			public DataCheck call(String data) throws Exception {
				DataCheck dCheck = new DataCheck(data, ",");
				return dCheck;
			}

		});
		//this.COLUMN_COUNT = 3;
		
		 * rowData.mapToPair(new PairFunction<Boolean,DataCheck>(){
		 * 
		 * @Override public DataCheck call(Boolean arg0) throws Exception { //
		 * TODO Auto-generated method stub return null; }
		 * 
		 * });
		 
		// JavaPairRDD<Boolean, String> output=
		
		 * rowData.mapToPair(new ColumnConstraint()).reduceByKey(new
		 * CheckPair()).saveAsHadoopFile("output1/", Boolean.class,
		 * String.class, RDDMultipleTextRowFormat.class);
		 
//		rowData.mapToPair(new ColumnConstraint()).saveAsHadoopFile("output1/",
//				NullWritable.class, String.class,
//				RDDMultipleTextRowFormat.class);
	}
*/
	
	public void validateSourceFile(String aSrcPath, String aDestPath,
			int colCount,final String aDelimiter,Column[] aColArr) {
		JavaSparkContext sc=null;
		try{
		SparkConf conf = new SparkConf().setAppName("Schema")
				.setMaster("local");
		int columnCount=aColArr.length;
		 sc= new JavaSparkContext(conf);
		JavaRDD<String> emp = sc.textFile(aSrcPath);

		JavaRDD<DataCheck> rowData = emp.map(new Function<String, DataCheck>() {
			@Override
			public DataCheck call(String data) throws Exception {
				DataCheck dCheck = new DataCheck(data, aDelimiter);
				return dCheck;
			}

		});
		
		log.info("Destination Path ----"+aDestPath);	
/*		rowData.mapToPair(new ColumnConstraint(columnCount))
		
		
		.saveAsHadoopFile("./temp/"+aDestPath,
				NullWritable.class, String.class,
				RDDMultipleTextRowFormat.class);
*/		
		}finally{
			if(sc!=null)
				sc.close();
		}
		
	}
	
}

/*class ColumnConstraint implements PairFunction<DataCheck, Boolean, String> {

	private static final Logger logs = Logger.getLogger(ColumnConstraint.class.getName());
	int colCount=0;
	ColumnConstraint(int aColNum){
		colCount=aColNum;
	}
	
	@Override
	public Tuple2<Boolean, String> call(DataCheck s) throws Exception {
		int col = s.getcColumns().size();
		logs.info("Check Column ----"+col);
		if (col == colCount)
			s.setValid(true);
		else
			s.setValid(false);

		return new Tuple2<Boolean, String>(s.isValid(), s.getcFormatValue());
	}

}
*/

