package com.dq.utils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dq.core.FAESourceLoader;

public class FAECli {

	private static final Logger log = Logger.getLogger(FAECli.class.getName());
	private String[] args = null;
	private Options options = new Options();

	public FAECli(){
		
	}
/*	public FAECli(String[] args) {
		this.args = args;
		options.addOption("v", "var", true, "Here you can set parameter .");
	}
*/
	public void parse() {
//		CommandLineParser parser = new BasicParser();
//		CommandLine cmd = null;
		try {
//			cmd = parser.parse(options, args);
			FAESourceLoader faeSrcLoad=new FAESourceLoader();
			faeSrcLoad.loadSchema("E:/Project/Workspace/dq/src/main/resources/sha_oncology.xml");
			faeSrcLoad.checkSource();
			//faeSrcLoad.executeDQ();
			
		} catch (Exception e) {
			log.log(Level.ERROR, "Failed to parse comand line properties", e);
		}
	}
	
	
	public static void main(String[] args){
		FAECli faeCli=new FAECli();
		faeCli.parse();
	}
}
