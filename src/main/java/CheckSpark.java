import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.dq.utils.FAEUtils;

public class CheckSpark {

	public static void main(String[] args) {
		 SparkConf conf = new SparkConf()
         .setAppName("Schema")
         .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext=new SQLContext(sc);
		JavaRDD<String> emp=sc.textFile("./employee.txt");
		String schemaStr="id name age";
		StructType sparkSchema = new StructType(new StructField[]{ 
	                new StructField("id", DataTypes.StringType, true, Metadata.empty()), 
	                new StructField("name", DataTypes.StringType, true, Metadata.empty()), 
	                new StructField("age", DataTypes.StringType, true, Metadata.empty())} 
	        );
		 
		 
		 JavaRDD<Row> rowData = emp.map(new Function<String, String[]>() {
				@Override
				public String[] call(String line) throws Exception {
					System.out.println(line);
					return line.split(",");
				}
			}).map(new Function<String[], Row>() {
				@Override
				public Row call(String[] r) throws Exception {
					return RowFactory.create(r[0], r[1], r[2]);
				}
			});
		 
		 DataFrame df=sqlContext.createDataFrame(rowData, sparkSchema);
		 df.printSchema();
		 System.out.println(df.columns().length);
		 df.show();
	}
	
	public static StructField[] extractFieldsFromString(String schemaString) {
	    String[] strFields = schemaString.split(",");
	    StructField[] resFields = new StructField[(strFields.length)];
	    String name, type;
	    String[] strFieldTokens;
	    for (int i = 0; i < strFields.length; i++) {
	        strFieldTokens = strFields[i].trim().split(" ");
	        name = strFieldTokens[0].trim();
	        type = strFieldTokens[1].trim();
	        StructField field = new StructField(name, FAEUtils.stringToDataType(type),
	                                            true, Metadata.empty());
	        resFields[i] = field;
	    }
	    return resFields;
	}
}
