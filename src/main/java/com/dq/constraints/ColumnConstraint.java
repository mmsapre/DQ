package com.dq.constraints;

import com.dq.core.DataCheck;
import com.dq.utils.FAEError;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.log4j.Logger;

import scala.Tuple2;


public class ColumnConstraint implements PairFunction<DataCheck, Boolean, DataCheck>{

	private static final Logger logs = Logger.getLogger(ColumnConstraint.class.getName());
	int colCount=0;


	public ColumnConstraint(int aColNum){
		colCount=aColNum;
	}
	
	
	@Override
	public Tuple2<Boolean, DataCheck> call(DataCheck s) throws Exception {
		int col = s.getcColumns().size();
		logs.info("Check Column ----"+col);
		if (col == colCount){
			s.setValid(true);
		}else{
			s.setValid(false);
			s.getcBadRowDescription().add(FAEError.DATA_FORMAT_ERROR.toString());
		}
		return new Tuple2<Boolean, DataCheck>(s.isValid(), s);
	}

}
