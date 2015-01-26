package com.thomsonreuters.ce.dbor.cfsdi.cursor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;






import oracle.jdbc.OracleTypes;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.cfsdi.Starter;
import com.thomsonreuters.ce.dbor.cfsdi.util.Counter;
import com.thomsonreuters.ce.dbor.server.DBConnNames;


public class CursorAction implements Runnable {

	private static final String GetCursor="{call sdi_commodity_flow_util_pkg.collect_data(?,?,?)}";
	private HashMap<Long, HashMap<String,FileDataMarker>> PERMID_CURSOR_MAP;
	private String CursorName;
	private boolean IsFull=true;
	private Counter counter;
	private String TempFileName = null;

	public CursorAction(String CursorName, HashMap<Long, HashMap<String,FileDataMarker>> pcm, Counter cut)
	{

		this.CursorName=CursorName;
		this.PERMID_CURSOR_MAP=pcm;
		this.counter=cut;
		
		try {
			File tempFile = File.createTempFile(this.CursorName, null, new File(Starter.TempFolder));
			this.TempFileName=tempFile.getName();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("IOException",e);
		}		
		finally
		{
			this.counter.Increase();
		}
	}
	
	public CursorAction(String CursorName,HashMap<Long, HashMap<String,FileDataMarker>> pcm, Counter cut,boolean isfull)
	{
		this(CursorName,pcm,cut);
		this.IsFull=isfull;
	}	
	
	

	public void run() {
		// TODO Auto-generated method stub

		Connection DBConn = null;

		try {

			DBConn = new EasyConnection(DBConnNames.CEF_CNR);
			CallableStatement objStatement=DBConn.prepareCall(GetCursor);
			objStatement.setString("cur_name_in", this.CursorName);
			objStatement.registerOutParameter("cur_out", OracleTypes.CURSOR);
			
			if (this.IsFull)
			{
				objStatement.setString("indicator_in", "F");
			}
			else
			{
				objStatement.setString("indicator_in", "I");
			}
			
			objStatement.execute();
			ResultSet result_set = (ResultSet) objStatement.getObject("cur_out");
			
			///////////////////////////
			//Get Column Names
			ResultSetMetaData rsmd = result_set.getMetaData(); 
			int count = rsmd.getColumnCount();
			String[] ColumnNameList=new String[count];		
			
			for (int i = 0; i < count; i++) { 
				String columnname=rsmd.getColumnName(i+1);
				ColumnNameList[i]=columnname;
			}
			///////////////////////////

			Long temp_perm_id=null;
			SDICursorRow[] SDICR=null;
			
			result_set.setFetchSize(10);
			while (result_set.next()) {		

				HashMap<String, Object> RowValues=new HashMap<String, Object>();
				
				for (int i=0; i<count;i++)	
				{
					Object value=null;
					
					if (rsmd.getColumnClassName(i+1)=="java.sql.Timestamp")
					{
						value = result_set.getTimestamp(i+1);
					}
					else
					{
						value=result_set.getObject(i+1);
					}
					
					String cn=ColumnNameList[i];
						
					if (value == null)
					{
						RowValues.put(cn, null);
					} 
					else if (value.getClass().getName()=="oracle.sql.STRUCT" )
					{
						Object[] attrs = ((oracle.sql.STRUCT)value).getAttributes();
						RowValues.put(cn, attrs);
					}
					else
					{
						RowValues.put(cn, value);
					}
				}
				
				SDICursorRow thisRow= new SDICursorRow(RowValues);
				
				Long perm_id=thisRow.getPerm_ID();

				if (!perm_id.equals(temp_perm_id))
				{
					if (temp_perm_id!=null)
					{
						FileDataMarker FDM=this.saveResultSet(SDICR);

						HashMap<String,FileDataMarker> HM;				

						synchronized(this.PERMID_CURSOR_MAP)
						{							
							HM=this.PERMID_CURSOR_MAP.get(temp_perm_id);

							if (HM==null)
							{
								HM=new HashMap<String,FileDataMarker>();
								this.PERMID_CURSOR_MAP.put(temp_perm_id, HM);						
							}
						}						

						synchronized(HM)
						{				

							HM.put(this.CursorName, FDM);							
						}						

					}

					SDICR=new SDICursorRow[0];
					temp_perm_id=perm_id;
				}

				SDICursorRow[] tempSDICR=new SDICursorRow[SDICR.length+1];
				System.arraycopy(SDICR, 0, tempSDICR, 0, SDICR.length);
				tempSDICR[SDICR.length]=thisRow;
				SDICR=tempSDICR;
			}

			objStatement.close();
			
			if (temp_perm_id!=null)
			{

				FileDataMarker FDM=this.saveResultSet(SDICR);

				HashMap<String,FileDataMarker> HM;				

				synchronized(this.PERMID_CURSOR_MAP)
				{							
					HM=this.PERMID_CURSOR_MAP.get(temp_perm_id);

					if (HM==null)
					{
						HM=new HashMap<String,FileDataMarker>();
						this.PERMID_CURSOR_MAP.put(temp_perm_id, HM);						
					}
				}						

				synchronized(HM)
				{				

					HM.put(this.CursorName, FDM);							
				}						

			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("Database exception",e);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("IO Exception",e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("Unknown Exception",e);
		} 		
		finally
		{
			try {		
				DBConn.close();				
			} catch (SQLException e) {
				Starter.cfLogger.error("Database exception",e);
			}
			Starter.cfLogger.info(this.CursorName + "------------------->Done");
			this.counter.Decrease();
		}

		
	}
	
	public SDICursorRow[] getResultSet(FileDataMarker FDM) throws IOException,ClassNotFoundException
	{

		File TempFile=new File(Starter.TempFolder,this.TempFileName);
		RandomAccessFile DiskCache = new RandomAccessFile(TempFile, "rw");
		
		DiskCache.seek(FDM.getPosition());
		byte[] OArray = new byte[FDM.getSize()];
		DiskCache.read(OArray);
		DiskCache.close();
		
		ByteArrayInputStream bOP = new ByteArrayInputStream(OArray);
		ObjectInputStream OIS=new ObjectInputStream(bOP);

		SDICursorRow[] SCR=(SDICursorRow[])OIS.readObject();
		OIS.close();
		return SCR;
	}
	
	
	
	
	public FileDataMarker saveResultSet(SDICursorRow[] SCR) throws IOException
	{
		File TempFile=new File(Starter.TempFolder,this.TempFileName);
		RandomAccessFile DiskCache = new RandomAccessFile(TempFile, "rw");
		
		ByteArrayOutputStream bOP = new ByteArrayOutputStream();
		ObjectOutputStream oO = new ObjectOutputStream( bOP);
		oO.writeObject(SCR);
		oO.flush();
		oO.close();
		
		byte[] OArray = bOP.toByteArray();
		int ObjectSize=OArray.length;
		
		long position=DiskCache.length();
		DiskCache.seek(position);
		DiskCache.write(OArray);
		DiskCache.close();
		
		return new FileDataMarker(this, position,ObjectSize);
		
	}
	
	public void Delete()
	{
		File TempFile=new File(Starter.TempFolder,this.TempFileName);
		TempFile.delete();
		
	}	


}
