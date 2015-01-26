package com.thomsonreuters.ce.dbor.cfsdi.util;

public class Counter {

	private int count=0;
	
	public void Increase()
	{
		synchronized(this)
		{
			this.count++;
		}
	}
	
	public void Decrease()
	{
		synchronized(this)
		{
			this.count--;
			
			if (this.count==0)
			{
				this.notify();
			}
		}
	}
	
	public void WaitToDone()
	{
		synchronized(this)
		{
			while(this.count>0)
			{
				try {
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
