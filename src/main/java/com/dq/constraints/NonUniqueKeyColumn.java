package com.dq.constraints;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class NonUniqueKeyColumn {

	private static Logger LOGGER = Logger.getLogger(NonUniqueKeyColumn.class);
	
	public DataFrame validate(DataFrame aDataFrame,String aColumnName){
		long cont=aDataFrame.groupBy(new Column(aColumnName)).count().filter("count >1").count();
		Row[] row=aDataFrame.groupBy(new Column(aColumnName)).count().filter("count >1").collect();
		//DataFrame dfNonUnique=aDataFrame.select(aDataFrame.col("*")).groupBy(new Column(aColumnName)).count().filter("count >1");
		DataFrame dfNonUnique=null;
		LOGGER.debug("Count of NonUniqueKeyColumn Column  ==="+cont);
		System.out.println("==============Unique count=============================="+cont);
		for(int i=0;i<row.length;i++){
			System.out.println("==============Unique Row Name=============================="+row[i]);
			DataFrame dfPartUnique=aDataFrame.where(aDataFrame.col(aColumnName).equalTo(row[i].get(0)).toString());
			if(dfNonUnique==null){
				System.out.println("==============Data Frame If =============================="+dfPartUnique.count());
				dfNonUnique=dfPartUnique;
			}else{
				System.out.println("==============Data Frame Else =============================="+dfPartUnique.count());
				dfPartUnique.show();
				dfNonUnique=dfNonUnique.unionAll(dfPartUnique);
				System.out.println("==============After Data Frame Else =============================="+dfNonUnique.count());
				dfNonUnique.show();
			}
			
		}
		dfNonUnique.show();
		return dfNonUnique;
	}
	
}
