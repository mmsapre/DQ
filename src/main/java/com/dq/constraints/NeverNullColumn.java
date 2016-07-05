package com.dq.constraints;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.log4j.Logger;

public class NeverNullColumn {

	private static Logger LOGGER = Logger.getLogger(NeverNullColumn.class);
	public NeverNullColumn(){
		
	}
	
	public DataFrame validate(DataFrame aDataFrame,String aColumnName){
		System.out.println("==================Null =Check =Before=========================");
		DataFrame dfNotNull=aDataFrame.filter(new Column(aColumnName).isNotNull());
		long colNullCount=dfNotNull.count();
		//long colEmpt=aDataFrame.filter(new Column(aColumnName).isNull()).count();
		LOGGER.debug("Count of Column null ==="+colNullCount);
		System.out.println("==================Null =Check =========================="+colNullCount);
		dfNotNull.show();
		///System.out.println("==================Empty =Check =========================="+dfNotNull.);
		return dfNotNull;
	}

}
