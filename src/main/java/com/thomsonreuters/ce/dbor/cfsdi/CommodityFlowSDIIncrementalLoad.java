package com.thomsonreuters.ce.dbor.cfsdi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.timing.DateConstants;
import com.thomsonreuters.ce.timing.DateFun;
import com.thomsonreuters.ce.timing.Schedule;
import com.thomsonreuters.ce.timing.ScheduleType;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.cache.SDIPublishStyle;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.CursorAction;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.FileDataMarker;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.SDICursorRow;
import com.thomsonreuters.ce.dbor.cfsdi.generator.CommodityFlowGenerator;
import com.thomsonreuters.ce.dbor.cfsdi.generator.TradeGenerator;
import com.thomsonreuters.ce.dbor.cfsdi.util.Counter;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.queue.MagicPipe;
import com.thomsonreuters.ce.database.EasyConnection;

public class CommodityFlowSDIIncrementalLoad extends ControlledThread {

	public static final String configfile="../cfg/cfsdi_incremental.conf";
	

	//SQLs to maintain file processing history
	private static final String CheckDBTime = "select to_char(sysdate,'yyyymmddhh24miss') from dual";
	private static final String InsertFileProcessHistory = "insert into file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";
	private static final String GetProcessingDetail = "select count(*) from processing_detail where fph_id = ? and dit_message_category_id in (select id from dimension_item where value='WARNING')";
	private static final String CompleteFileHistory = "update file_process_history set end_time=sysdate, dit_processing_status=? where id=?";
	private static final String InsertSDIDetail = "insert into sdi_file_detail (fph_id, dit_publish_style_id, uuid,sdi_timestamp,incremental_date, previous_date) values (?,?,?,?,to_date(?,'yyyymmddhh24miss'),to_date(?,'yyyymmddhh24miss'))";
	private static final String CheckLastIncrementalTime="select to_char(incremental_date,'yyyymmddhh24miss') as incremental_date from (select b.incremental_date as incremental_date from file_process_history a, sdi_file_detail b where a.id=b.fph_id and a.dit_file_category_id=? and b.Dit_Publish_Style_Id=? and a.end_time is not null order by b.incremental_date desc) where rownum=1";

	//SQLS to retrieve SDI underlying data from DB;
	private static final String InitialPermIDList="{call sdi_commodity_flow_util_pkg.initialise_perm_id_lst(?,?,'I')}";
	
	private static Date EndSDITimeStamp=null;

	private char scheduletype;

	private String scheduletime;

	private long interval;

	private long offset;

	private String sdiFileLocation;
	
	private int CursorRowDispatcher;
	
	private int CommodityFlowGenerator;
	
	private int TradeGenerator;
	

	// Configuration variables
	public HashMap<Long, HashMap<String,FileDataMarker>> PERMID_CURSOR_MAP;
	
	public CommodityFlowSDIIncrementalLoad(ThreadController tc)
	{
		super(tc);
	}
	
	public void ControlledProcess() {
		
		// TODO Auto-generated method stub
		//Read configuration file
		try {
			FileInputStream TaskFis = new FileInputStream(configfile);
			Properties Prop = new Properties();
			Prop.load(TaskFis);

			scheduletype = Prop.getProperty("scheduletype").toCharArray()[0];;
			scheduletime = Prop.getProperty("time");
			interval = Integer.parseInt(Prop.getProperty("interval"));
			offset = Integer.parseInt(Prop.getProperty("offset"));
			sdiFileLocation = FileUtilities.GetAbsolutePathFromEnv(Prop.getProperty("filelocation"));
			CursorRowDispatcher=Integer.parseInt(Prop.getProperty("CursorRowDispatcher"));
			CommodityFlowGenerator=Integer.parseInt(Prop.getProperty("CommodityFlowGenerator"));
			TradeGenerator=Integer.parseInt(Prop.getProperty("TradeGenerator"));
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("NumberFormatException",e);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("FileNotFoundException",e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("IOException",e);
		}	

		if (EndSDITimeStamp==null)
		{
			//To determine first SDI run time after feed starts up
			Date FirstRunningTime=CalculateFirstRunningTime();
			Starter.cfLogger.info("[commodity flows SDI incremental load]: Next execute time on SFS:"+DateFun.getStringDate(FirstRunningTime));
			
			CommodityFlowSDIIncrementalLoad SSIL=new CommodityFlowSDIIncrementalLoad(this.TC);
			Starter.TimerService.createTimer(FirstRunningTime, 0, SSIL);			
		}
		else
		{			
			//Calculate start time stamp
			Date StartSDITimeStamp=null;
			
			Connection Conn = new EasyConnection(DBConnNames.CEF_CNR);			
			try {
				PreparedStatement PreStatement = null;
				ResultSet objResult = null;
				PreStatement = Conn.prepareStatement(CheckLastIncrementalTime);
				PreStatement.setInt(1, FileCategory.getInstance("COMMODIFY FLOW SDI").getID());
				PreStatement.setInt(2, SDIPublishStyle.INCREMENTAL.getID());
				
				objResult = PreStatement.executeQuery();
				
				if (objResult.next())
				{
					SimpleDateFormat formatter = new SimpleDateFormat(DateConstants.FULLTIMEFORMAT); 
					ParsePosition pos = new ParsePosition(0); 
					StartSDITimeStamp= formatter.parse(objResult.getString("incremental_date"), pos);
				}					
				else
				{
					StartSDITimeStamp= new Date(EndSDITimeStamp.getTime()-interval);
				}
				
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("SQL exception",e1);
			} finally {
				try {
					Conn.close();
				} catch (SQLException e) {
					Starter.cfLogger.error("SQL exception",e);
				}
			}			
			
			
			Starter.cfLogger.info("[commodity flows SDI incremental load]: Start Incremental SDI generation for timestamp: "+ DateFun.getStringDate(StartSDITimeStamp)+"."+DateFun.getStringDate(EndSDITimeStamp));
			//Clear PERMID_CURSOR_MAP;
			PERMID_CURSOR_MAP = new HashMap<Long, HashMap<String,FileDataMarker>>();

			//Generate Manifest
			String temp_FileSuffix=getStringDate(StartSDITimeStamp)+"."+getStringDate(EndSDITimeStamp)+".Incremental";
			String ManiFest_Name=SDIConstants.ManiFest_Prefix+temp_FileSuffix;
			String CommodityFlows_CommodityFlow_Name=SDIConstants.CommodityFlow_Prefix + temp_FileSuffix+".xml.gz";
			String CommodityFlows_Trade_Name=SDIConstants.Trade_Prefix + temp_FileSuffix+".xml.gz";

			//Log a file processing record in table
			long FPH_ID=CreateFileProcessHistory(ManiFest_Name);

			//////////////////////////////////////////////
			//Added by Jing Wang
			//create new incremental record in table EM_SDI_HISTORY
			
			Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);				
			try {
				PreparedStatement objPreStatement = DBConn.prepareStatement(InsertSDIDetail);
				objPreStatement.setLong(1, FPH_ID);
				objPreStatement.setInt(2, SDIPublishStyle.INCREMENTAL.getID());
				objPreStatement.setString(3,temp_FileSuffix );
				objPreStatement.setDate(4, new java.sql.Date(new Date().getTime()));
				SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
				objPreStatement.setString(5, f.format(EndSDITimeStamp));
				objPreStatement.setString(6, f.format(StartSDITimeStamp));
				objPreStatement.executeUpdate();
				DBConn.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("Database error",e);
			} finally
			{
				try {
					DBConn.close();
				} catch (SQLException e) {
					Starter.cfLogger.error("Database error",e);
				}
			}
			

			long StartTime=new Date().getTime();

			//////////////////////////////////////////////
			//Step 1 read asset underlying data used for assembling XML from cursors into location disk files

			try {
				DBConn = new EasyConnection(DBConnNames.CEF_CNR);

				//Initialize PermID List
				CallableStatement objStatement=DBConn.prepareCall(InitialPermIDList);
				objStatement.setTimestamp(1, new Timestamp(StartSDITimeStamp.getTime()));
				objStatement.setTimestamp(2, new Timestamp(EndSDITimeStamp.getTime()));
				objStatement.execute();
				objStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("Database exception",e);
			}
			finally
			{
				try {
					DBConn.close();
				} catch (SQLException e) {
					Starter.cfLogger.error("Database exception",e);
				}	
			}

			

			try {
				///////////////////////////////////////
				//unload cursor data to local dump files
				Counter CursorActionCT=new Counter();
				
				CursorAction CA_CFCI=new CursorAction(CursorType.COMMODITY_FLOW_COMMON_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);                       
				CursorAction CA_CFOI=new CursorAction(CursorType.COMMODITY_FLOW_ORGANISATION_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CFCMI=new CursorAction(CursorType.COMMODITY_FLOW_COMMODITY_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CFLDI=new CursorAction(CursorType.COMMODITY_FLOW_LOAD_DISCH_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CFADI=new CursorAction(CursorType.COMMODITY_FLOW_ARRIVE_DEPARTURE_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CFLI=new CursorAction(CursorType.COMMODITY_FLOW_LINKAGE_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CFFRI=new CursorAction(CursorType.COMMODITY_FLOW_FLOW_RSHIP_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CTCI=new CursorAction(CursorType.COMMODITY_TRADE_COMMON_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				CursorAction CA_CTOI=new CursorAction(CursorType.COMMODITY_TRADE_ORGANISATION_INFO,PERMID_CURSOR_MAP,CursorActionCT,false);
				
				new Thread(CA_CFCI).start();   
				new Thread(CA_CFOI).start(); 
				new Thread(CA_CFCMI).start(); 
				new Thread(CA_CFLDI).start(); 
				new Thread(CA_CFADI).start(); 
				new Thread(CA_CFLI).start(); 
				new Thread(CA_CFFRI).start();         
				new Thread(CA_CTCI).start();
				new Thread(CA_CTOI).start();

				///////////////////////////////////////
				//waiting for cursor unload to complete
				CursorActionCT.WaitToDone();
				
				
				long TimeSpent=new Date().getTime()-StartTime;
				Starter.cfLogger.info("[commodity flows SDI incremental load]: Time spent on unloading cursors-------------------"+TimeSpent);

				//start dispatcher
				MagicPipe<HashMap<String, SDICursorRow[]>> MP_CommodityFlow=new MagicPipe<HashMap<String, SDICursorRow[]>>(500, 1000,	1024, 50, Starter.TempFolder);
				MagicPipe<HashMap<String, SDICursorRow[]>> MP_Trade=new MagicPipe<HashMap<String, SDICursorRow[]>>(500, 1000,	1024, 50, Starter.TempFolder);
					
				CursorRowDispatcher CRD=new CursorRowDispatcher(CursorRowDispatcher,MP_CommodityFlow,MP_Trade);
				CRD.start(PERMID_CURSOR_MAP.entrySet().iterator());
				
				//clear thread counter
				CursorActionCT=new Counter();
				
                CommodityFlowGenerator CPAG=new CommodityFlowGenerator(sdiFileLocation, CommodityFlows_CommodityFlow_Name, StartSDITimeStamp, EndSDITimeStamp, CommodityFlowGenerator,MP_CommodityFlow,CursorActionCT);
                CPAG.Start();
                TradeGenerator TG=new TradeGenerator(sdiFileLocation, CommodityFlows_Trade_Name, StartSDITimeStamp, EndSDITimeStamp, TradeGenerator,MP_Trade,CursorActionCT);
                TG.Start();
               
				//Wait for all all file generators finishing work.
                CursorActionCT.WaitToDone();
                
                CA_CFCI.Delete();
                CA_CFOI.Delete();
                CA_CFCMI.Delete();
                CA_CFLDI.Delete();
                CA_CFADI.Delete();
                CA_CFLI.Delete();
                CA_CFFRI.Delete();
                CA_CTCI.Delete();
                CA_CTOI.Delete();
                
                Starter.cfLogger.info("[commodity flows SDI incremental load]: All cursor dump files are removed");

				//update status to success
				CompleteFileHis(FPH_ID);
				
				//Commodity Flows Done File
				BufferedWriter CFout = new BufferedWriter(new FileWriter(new File(sdiFileLocation ,CommodityFlows_CommodityFlow_Name+".done")));
				CFout.close();
				
				//Trade Done File
				BufferedWriter Tout = new BufferedWriter(new FileWriter(new File(sdiFileLocation ,CommodityFlows_Trade_Name+".done")));
				Tout.close();
				
				TimeSpent=new Date().getTime()-StartTime;
				Starter.cfLogger.info("[commodity flows SDI incremental load]: Time spent on generating SDI files-------------------"+TimeSpent);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("IOException",e);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("Unknown Exception",e);
			} 

			
			//Calculate next running time			
			EndSDITimeStamp= new Date (EndSDITimeStamp.getTime()+interval);
			long timediff = getDBDate().getTime()- (new Date()).getTime();

			Date nextExecuteTime=new Date(EndSDITimeStamp.getTime()-timediff+offset);
			Starter.cfLogger.info("[commodity flows SDI incremental load]: Next execute time on SFS:"+DateFun.getStringDate( nextExecuteTime));
			
			CommodityFlowSDIIncrementalLoad SSIL=new CommodityFlowSDIIncrementalLoad(this.TC);
			Starter.TimerService.createTimer(nextExecuteTime,0, SSIL);
			
		}
	}

	
	
	public Date CalculateFirstRunningTime()	
	{		
		//Get DB and APP Server Time
		Date DBDate=getDBDate();
		Date AppDate=new Date();

		//Get time diff
		long timediff = DBDate.getTime()- AppDate.getTime();

		//Calculate next validate DB time;
		EndSDITimeStamp=(new Schedule(DBDate,ScheduleType.getInstance(scheduletype),scheduletime)).GetNextValidTime();

		//Calculate next validate APP time
		Date nextExecuteTime=new Date(EndSDITimeStamp.getTime()-timediff+offset);

		while(nextExecuteTime.before(AppDate))
		{

			EndSDITimeStamp= new Date(EndSDITimeStamp.getTime()+interval);
			nextExecuteTime=new Date(EndSDITimeStamp.getTime()-timediff+offset);
		}

		return nextExecuteTime;
	}

	public long CreateFileProcessHistory(String filename)
	{
		Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
		long FPH_ID=0;

		try {
			DatabaseMetaData dmd = DBConn.getMetaData();
			PreparedStatement objPreStatement = DBConn.prepareStatement(InsertFileProcessHistory, new String[]{"ID"});
			objPreStatement.setString(1, filename);
			objPreStatement.setInt(2, FileCategory.getInstance("COMMODIFY FLOW SDI").getID());
			objPreStatement.setInt(3, ProcessingStatus.PROCESSING.getID());

			objPreStatement.executeUpdate();

			//get ID			
			if(dmd.supportsGetGeneratedKeys()) {   
				ResultSet rs = objPreStatement.getGeneratedKeys();   
				while(rs.next()) {
					FPH_ID=rs.getLong(1);
				}
			}

			DBConn.commit();
			objPreStatement.close();

		}
		catch (SQLException e) {
			Starter.cfLogger.error("Database exception",e);

		} finally {
			try {
				DBConn.close();
			} catch (SQLException e) {
				Starter.cfLogger.error("Database exception",e);
			}
		}		

		return FPH_ID;
	}

	private Date getDBDate()
	{
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
		PreparedStatement objPreStatement = null;
		ResultSet objResult = null;

		try {
			////////////////////////////////////////////////////////////////////////
			//get attributes
			objPreStatement = DBConn.prepareStatement(CheckDBTime);
			objResult = objPreStatement.executeQuery();

			objResult.next();
			String strDBDate = objResult.getString(1);

			SimpleDateFormat formatter = new SimpleDateFormat(DateConstants.FULLTIMEFORMAT); 
			ParsePosition pos = new ParsePosition(0); 
			Date DBDate = formatter.parse(strDBDate, pos);

			objResult.close();
			objPreStatement.close();

			return DBDate;

		} catch (SQLException e) {
			Starter.cfLogger.error("Database exception",e);
			return null;
			
		} finally
		{
			try {
				DBConn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("Database exception",e);
			}		
		}
	}

	public String getStringDate(Date time) { 

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HHmm"); 
		String dateString = formatter.format(time); 
		return dateString; 
	}

	public void CompleteFileHis(long FPH_ID)
	{
		Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

		try {
			// if processint_detail table has records, then it's
			// COMPLETEDWITHWARNING
			int pdeCount=-1;
			PreparedStatement getPdetPreStatement = DBConn
			.prepareStatement(GetProcessingDetail);
			getPdetPreStatement.setLong(1, FPH_ID);
			ResultSet objResultSet = getPdetPreStatement.executeQuery();
			if (objResultSet.next()) {
				pdeCount = objResultSet.getInt(1);
			}

			objResultSet.close();
			getPdetPreStatement.close();

			PreparedStatement objPreStatement = null;
			objPreStatement = DBConn.prepareStatement(CompleteFileHistory);

			if (pdeCount <= 0) {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());
			} else {
				objPreStatement.setInt(1, ProcessingStatus.COMPLETEDWITHWARNING
						.getID());
			}			

			objPreStatement.setLong(2,FPH_ID);
			objPreStatement.executeUpdate();
			DBConn.commit();
			objPreStatement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("Database exception",e);
		}finally
		{
			try {
				DBConn.close();
			} catch (SQLException e) {
				Starter.cfLogger.error("Database exception",e);
			}												
		}			
	}	

}
