package com.dq.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.dq.core.DQValidations;

public class FAEUtils {

	private static final Log log = LogFactory.getLog(FAEUtils.class);

	public static DataType stringToDataType(String strType) {
		switch (strType.toLowerCase()) {
		case FAEConstants.INTEGER_TYPE:
			return DataTypes.IntegerType;
		case FAEConstants.INT_TYPE:
			return DataTypes.IntegerType;
		case FAEConstants.FLOAT_TYPE:
			return DataTypes.FloatType;
		case FAEConstants.DOUBLE_TYPE:
			return DataTypes.DoubleType;
		case FAEConstants.LONG_TYPE:
			return DataTypes.LongType;
		case FAEConstants.BOOLEAN_TYPE:
			return DataTypes.BooleanType;
		case FAEConstants.STRING_TYPE:
			return DataTypes.StringType;
		case FAEConstants.BINARY_TYPE:
			return DataTypes.BinaryType;
		default:
			log.error("Unresolved DataType: " + strType);
			throw new RuntimeException("Invalid DataType: " + strType);
		}
	}

	public static DQValidations stringToValidationType(String valType) {
		switch (DQValidations.valueOf(valType.toUpperCase())) {
		case NOTNULL:
			return DQValidations.NOTNULL;
		case UNIQUE:
			return DQValidations.UNIQUE;
		default:
			log.error("Unresolved Validation Type: " + valType);
			throw new RuntimeException("Invalid Validation Type: " + valType);
		}
	}

	public static void merge(String aSrcPath, String aDestPath) {
		Configuration config = new Configuration();
		try {
			FileSystem hdfsSys = FileSystem.get(config);
			FileUtil.copyMerge(hdfsSys, new Path(aSrcPath), hdfsSys, new Path(
					aDestPath), false, config, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
