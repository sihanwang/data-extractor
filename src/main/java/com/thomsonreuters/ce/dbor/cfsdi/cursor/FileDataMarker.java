package com.thomsonreuters.ce.dbor.cfsdi.cursor;

import java.io.IOException;


public class FileDataMarker {
	
	private long Position;
	private int Size;
	private CursorAction CA;
	
	
	public FileDataMarker(CursorAction ca, long pos, int size)
	{
		this.CA=ca;
		this.Position=pos;
		this.Size=size;
	}

	public long getPosition() {
		return Position;
	}

	public int getSize() {
		return Size;
	}
	
	public SDICursorRow[] getResultSet() throws IOException,ClassNotFoundException
	{
		return CA.getResultSet(this);
	}
	

}
