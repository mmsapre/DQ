import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;


public class ColumnCount {

	public static void main(String[] args) {
		 SparkConf conf = new SparkConf()
        .setAppName("Schema")
        .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//SQLContext sqlContext=new SQLContext(sc);
		JavaRDD<String> emp=sc.textFile("./employee.txt");
		List<Tuple2<Integer,Integer>> output=emp.mapToPair(new CountCsvColumns()).reduceByKey(new AddIntegerPair()).collect();
		
		for(Tuple2<Integer,Integer> tuple :output){
			System.out.println(tuple._1()+":::::"+tuple._2());
		}

	}
}

class CountCsvColumns implements PairFunction<String,Integer,Integer>{

	@Override
	public Tuple2<Integer, Integer> call(String s) throws Exception {
		String parts[]=s.split(",");
		int colCount=0;
		for(int i=0;i<parts.length;i++){
			if(parts[i]!=null&& !parts[i].isEmpty())
				colCount+=1;
		}
		return new Tuple2<Integer,Integer>(colCount,1);
	}
	
}

class AddIntegerPair implements Function2<Integer,Integer,Integer>{


	@Override
	public Integer call(Integer a, Integer b) throws Exception {
		// TODO Auto-generated method stub
		return a+b;
	}
	
}