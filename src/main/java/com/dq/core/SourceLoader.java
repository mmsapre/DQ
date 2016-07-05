package com.dq.core;

public interface SourceLoader {
	
	public void loadSchema(String aPath);
	public void checkSource();
	public void executeDQ();
	public void loadToHDFS();
}
