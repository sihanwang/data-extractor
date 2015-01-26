package com.thomsonreuters.ce.dbor.cfsdi;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.thomsonreuters.ce.dbor.cfsdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.FileDataMarker;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.SDICursorRow;
import com.thomsonreuters.ce.queue.MagicPipe;


public class CursorRowDispatcher {
	
	private int thread_num;
	
	private int ActiveNum=0;
	private MagicPipe<HashMap<String, SDICursorRow[]>> MP_CF;
	private MagicPipe<HashMap<String, SDICursorRow[]>> MP_T;

	

	public CursorRowDispatcher(int Thread_Num, 
			MagicPipe<HashMap<String, SDICursorRow[]>> mp_cf,MagicPipe<HashMap<String, SDICursorRow[]>> mp_t)

	{
		this.thread_num=Thread_Num;
		this.MP_CF=mp_cf;		
		this.MP_T=mp_t;

	}
	
	public void start(Iterator<Map.Entry<Long, HashMap<String,FileDataMarker>>> iter)
	{
		
		Thread[] ThreadArray=new Thread[this.thread_num];
		for (int i = 0; i < this.thread_num; i++) {
			ThreadArray[i] = new Thread( new Worker(iter));			
		}	
		
		for (int i = 0; i < this.thread_num; i++) {
			ThreadArray[i].start();		
		}	
		
		
	}
	
	private class Worker implements Runnable {
		
		private Iterator<Map.Entry<Long, HashMap<String,FileDataMarker>>> iter;
		
		public Worker(Iterator<Map.Entry<Long, HashMap<String,FileDataMarker>>> ITER)
		{
			this.iter=ITER;
			
			synchronized (CursorRowDispatcher.this) {
				CursorRowDispatcher.this.ActiveNum++;
			}
		}


		public void run() {
			
			while(true)
			{
				try {
					
					HashMap<String,FileDataMarker> CursorData;
					
					synchronized(CursorRowDispatcher.this)
					{
						if (this.iter.hasNext()) {				
							Map.Entry<Long, HashMap<String,FileDataMarker>> element=this.iter.next();
							CursorData=element.getValue();
							this.iter.remove();
						}
						else
						{
							break;
						}
					}
					
					HashMap<String,SDICursorRow[]> All_Cursors=new HashMap<String,SDICursorRow[]>();
					
					for (Iterator<Map.Entry<String,FileDataMarker>> Asset_Iter=CursorData.entrySet().iterator(); Asset_Iter.hasNext();)
					{
						Map.Entry<String,FileDataMarker> element=Asset_Iter.next();
						
						String CN=element.getKey();
						FileDataMarker FDM=element.getValue();
						
						All_Cursors.put(CN, FDM.getResultSet());
						
					}
					
					SDICursorRow[] commodity_flow_common_info = All_Cursors.get(CursorType.COMMODITY_FLOW_COMMON_INFO);
					long commodity_flow_id=commodity_flow_common_info[0].getPerm_ID();
					Date entity_create_date=commodity_flow_common_info[0].getTimestamp("ENTITY_CREATE_DATE");
					String admin_status=commodity_flow_common_info[0].getString("ADMIN_STATUS");
					Date admin_status_effective_from=commodity_flow_common_info[0].getTimestamp("ADMIN_STATUS_EFFECTIVE_FROM");

					if (entity_create_date == null)
					{
						Starter.cfLogger.warn("Get empty entity_create_date for commodity flow id:"+commodity_flow_id);
						
					} else if (admin_status == null)
					{
						Starter.cfLogger.warn("Get empty admin_status for commodity flow id:"+commodity_flow_id);
						
					} else if (admin_status_effective_from ==null)
					{
						Starter.cfLogger.warn("Get empty admin_status_effective_from for commodity flow id:"+commodity_flow_id);
					}
					else
					{
						CursorRowDispatcher.this.MP_CF.putObj(All_Cursors);
						CursorRowDispatcher.this.MP_T.putObj(All_Cursors);
					}

					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					Starter.cfLogger.error("IOException",e);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					Starter.cfLogger.error("ClassNotFoundException",e);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					Starter.cfLogger.error("Unknown Exception",e);
				} 
				
			}
			
			
			synchronized (CursorRowDispatcher.this) {
				CursorRowDispatcher.this.ActiveNum--;
				
				if (CursorRowDispatcher.this.ActiveNum==0)
				{
					CursorRowDispatcher.this.MP_CF.Shutdown(true);
					CursorRowDispatcher.this.MP_T.Shutdown(true);
					Starter.cfLogger.info("Dispatcher is done!");

				}

			}
			
		}
	}	
	
	
}
