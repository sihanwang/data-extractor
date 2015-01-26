package com.thomsonreuters.ce.dbor.cfsdi;

import java.io.FileInputStream;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.dbor.file.FileUtilities;

import com.thomsonreuters.ce.dbor.server.SrvControl;

import com.thomsonreuters.ce.thread.ThreadController;
import com.thomsonreuters.ce.timing.TimerPool;


public class Starter implements SrvControl {
	/////////////////////////////////////////////////////////////////////////////////
	// config file path
	/////////////////////////////////////////////////////////////////////////////////
	private static final String Config_File = "../cfg/app.conf";
	
	public static TimerPool TimerService=null;
	
	public ThreadController TC=null;

	public static final String SERVICE_NAME="cfsdi";
	
	public static String TempFolder=null;
	
	public static Logger cfLogger=null;
	
	public static Starter cfsdi=null;
	
	public void Start(Properties prop)
	{

		/////////////////////////////////////////////////////////////////////////////
		// Initialize logging
		/////////////////////////////////////////////////////////////////////////////
		String loggingCfg = prop.getProperty("logging.configuration");
		PropertyConfigurator.configure(loggingCfg);
		cfLogger=Logger.getLogger(SERVICE_NAME);
		cfLogger.info("Logging is working");
		
		
		/////////////////////////////////////////////////////////////////////////////
		//Initialize database connection pool
		/////////////////////////////////////////////////////////////////////////////
		String db_Config_file = prop.getProperty("dbpool.configuration");
		EasyConnection.configPool(db_Config_file);
		cfLogger.info("Database connection pool is working");
		
		/////////////////////////////////////////////////////////////////////////////
		// start Timer Service
		/////////////////////////////////////////////////////////////////////////////
		TimerService = new TimerPool(1);
		TimerService.Start();
		
		cfLogger.info("Timer service is working");
		
		/////////////////////////////////////////////////////////////////////////////
		// Temp folder
		/////////////////////////////////////////////////////////////////////////////
		TempFolder=FileUtilities.GetAbsolutePathFromEnv(prop.getProperty("tempfolder"));
		
		
		TC=new ThreadController();		
		
		/////////////////////////////////////////////////////////////////////////////
		//Start Incremental SDI
		/////////////////////////////////////////////////////////////////////////////

		Properties cfisdi_prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(CommodityFlowSDIIncrementalLoad.configfile);
			cfisdi_prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read Incremental SDI configuration file: " + CommodityFlowSDIIncrementalLoad.configfile);
		}

		String IsEnabled=cfisdi_prop.getProperty("IsEnabled");
		if (IsEnabled.equals("true"))
		{
			CommodityFlowSDIIncrementalLoad CFSIL= new CommodityFlowSDIIncrementalLoad(TC);
			new Thread(CFSIL).start();
		}
		
		/////////////////////////////////////////////////////////////////////////////
		//Start Full SDI
		/////////////////////////////////////////////////////////////////////////////

		Properties cffsdi_prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(CommodityFlowSDIFullLoad.configfile);
			cffsdi_prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read Full SDI configuration file: " + CommodityFlowSDIFullLoad.configfile);
		}

		IsEnabled=cffsdi_prop.getProperty("IsEnabled");
		if (IsEnabled.equals("true"))
		{
			CommodityFlowSDIFullLoad CFSFL= new CommodityFlowSDIFullLoad(TC);
			new Thread(CFSFL).start();
		}

		
	}
	
	public void Stop()
	{
		//Normal shutdown

		TimerService.Stop();
		cfLogger.info("Timer service is down");

		TC.Shutdown();
		cfLogger.info("Shutdown signal is sent");

		TC.WaitToDone();
		cfLogger.info("All threads are done");

		EasyConnection.CloseAllPool();
		cfLogger.info("DBPool is closed");

		cfLogger.info("Feed is put down as requested");
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
        if (args.length > 0 && "stop".equals(args[0])) {
        	cfsdi.Stop();
            System.exit(0);
        }

		
		/////////////////////////////////////////////////////////////////////////////
		// Read config file into prop object
		/////////////////////////////////////////////////////////////////////////////
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(Config_File);
			prop.load(fis);
		} catch (Exception e) {
			System.out.println("Can't read configuration file: " + Config_File);
		}
		
		//Start service
		cfsdi=new Starter();
		cfsdi.Start(prop);


		Starter.cfLogger.info(SERVICE_NAME+ " is working now!");
	}



}
