package com.dq.core;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.Logger;

import com.dq.constraints.ColumnCheck;
import com.dq.mappings.Column;
import com.dq.mappings.Source;
import com.dq.mappings.SourceMappingReader;
import com.dq.mappings.Table;
import com.dq.utils.FAEUtils;

public class FAESourceLoader implements SourceLoader {
	private static final Logger log = Logger.getLogger(FAESourceLoader.class
			.getName());
	private List<Table> cTableLst = null;
	private Source cSource = null;

	@Override
	public void loadSchema(String aPath) {
		SourceMappingReader srcReader = new SourceMappingReader();
		cSource = srcReader.sourceMappingLoader(aPath);
	}

	@Override
	public void checkSource() {
		String lDelimit = cSource.getDelimiter();
		String lSourcePath = cSource.getSourceLocation();
		String lTargetPath = cSource.getTargetLocation();
		String lTargetFileName = cSource.getTargetName();
		cTableLst = cSource.getTable();
		log.info("Data Delimiter ::" + lDelimit + "::");
		System.out.println("Data Delimiter ::" + lDelimit + "::");
		ColumnCheck colCheck = new ColumnCheck();
		DQValidator dqValid = new DQValidator();
		Column[] arrCol = null;

		for (Table ltbl : cTableLst) {

			List<Column> lColumnLst = ltbl.getColumn();
			arrCol = new Column[lColumnLst.size()];
			lColumnLst.toArray(arrCol);
			dqValid.validate(lSourcePath, lTargetPath, lTargetFileName,
					arrCol.length, lDelimit, arrCol);
			/*
			 * colCheck.validateSourceFile(lSourcePath, lTargetPath,
			 * cTableLst.size(), lDelimit, arrCol);
			 */

		}

	}

	@Override
	public void executeDQ() {
		String lDelimit = cSource.getDelimiter();
		String lSourcePath = cSource.getSourceLocation();
		String lTargetPath = cSource.getTargetLocation();
		cTableLst = cSource.getTable();
		Column[] arrCol = null;
		log.info("DataQlty Delimiter ::" + lDelimit + "::");
		System.out.println("DataQlty Delimiter ::" + lDelimit + "::");
		DQValidator dqValid = new DQValidator();
		for (Table ltbl : cTableLst) {

			List<Column> lColumnLst = ltbl.getColumn();
			arrCol = new Column[lColumnLst.size()];
			lColumnLst.toArray(arrCol);
			/*
			 * dqValid.getValidationCheck("./temp/" + lTargetPath + "/TRUE",
			 * lTargetPath, "\t", arrCol);
			 */

			FileUtil.fullyDelete(new File("./temp/"));
			FAEUtils.merge("./tier", "./tier");
			FileUtil.fullyDelete(new File("./tier-1/NonDistinct/merge-tier"));
			FAEUtils.merge("./tier-1/NonDistinct/tier",
					"./tier-1/NonDistinct/merge-tier");

		}
	}

	@Override
	public void loadToHDFS() {
		// TODO Auto-generated method stub

	}

}
