package com.dq.constraints;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.log4j.Logger;

public class UniqueKeyColumn {

	private static Logger LOGGER = Logger.getLogger(UniqueKeyColumn.class);

	public UniqueKeyColumn() {

	}

	public DataFrame validate(DataFrame aDataFrame, String aColumnName) {
		DataFrame dfDistinct = aDataFrame
				.dropDuplicates(new String[] { aColumnName });
		return dfDistinct;
	}

}
