package com.dq.constraints;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrame;

public class NumberOfColumn {

	private int cColumnCount;
	private int cExpectedColumn;
	
	public NumberOfColumn(int expectedColumn){
		this.cExpectedColumn=expectedColumn;
	}
	
	public int apply(){
		return 0;
	}
}
