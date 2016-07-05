package com.dq.constraints;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.log4j.Logger;
import org.apache.spark.sql.functions;
public class NumberOfRow {

	private static Logger LOGGER = Logger.getLogger(NumberOfRow.class);
	
	
	public static void validate(DataFrame aDataFrame,String aColumnName,long expectedCount){
		long checkCnt=aDataFrame.count();
		long colRowCount=aDataFrame.agg(functions.count(new Column("*"))).count();
		LOGGER.debug("Count of row ==="+colRowCount);
		System.out.println("===========row---================================="+colRowCount+":::checkCnt::"+checkCnt);
	}
}
