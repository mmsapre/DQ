import java.util.Arrays;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

class RDDMultipleTextOutputFormat<Text, NullWritable> extends MultipleTextOutputFormat<Text, NullWritable>  {

	 @Override
	    protected String generateFileNameForKeyValue(Text key, NullWritable value, String name) {
	        return key.toString();
	    }
}

public class TestSpark {

    public static void main(String[] args) {
    	//System.setProperty("hadoop.home.dir", "E:/Download/hadoop-common/hadoop-common-2.2.0-bin-master");
        SparkConf conf = new SparkConf()
                .setAppName("SplitJob")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String[] strings = {"Abcd", "Azlksd", "whhd", "wasc", "aDxa"};
        String s=null;
        sc.parallelize(Arrays.asList(strings)).mapToPair(new PairFunction<String,String,String>(){

			public Tuple2<String, String> call(String s) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<>(s.substring(0,1).toLowerCase(), s);
			}
        	
        })
                .saveAsHadoopFile("output/", String.class, String.class,
                        RDDMultipleTextOutputFormat.class);
        sc.stop();
    }
    
}
