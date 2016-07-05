import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.dq.core.DQSparkUtil;


public class SparkXMLReader {

	public static void main(String[] args){
		
		//JavaSparkContext jsc=DQSparkUtil.createSparkContext("XMLReader", true);
		SQLContext sqlCtxt=DQSparkUtil.createSparkSQLContext("XMLReader", true);
		DataFrame df=sqlCtxt.read().format("com.databricks.spark.xml").option("rowTag", "table").load("E:/Project/Workspace/dq/src/main/resources/sha_oncology.xml");
		df.show();
	}
}
