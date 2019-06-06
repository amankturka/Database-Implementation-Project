package bufmgr;

import java.*;
import java.awt.List;
import java.util.ArrayList;

import global.*;

public class Clock implements GlobalConst {

	protected ArrayList<FrameDesc> buffer;
	protected int current;
	
	public Clock() {
		// TODO Auto-generated constructor stub
		this.buffer = null;
		this.current = 0;
	}
	
	public FrameDesc pickVictim(BufMgr bufMgr) {
		this.buffer = new ArrayList<FrameDesc>(bufMgr.mapPageFrame.values());
		 for(int i = 0;i<(buffer.size()*2);i++)
		 {
			 if(buffer.get(current).valid != true) {
				 return buffer.get(current);
			 }
			 else {
				if(buffer.get(current).pincount == 0) {
					if(buffer.get(current).refbit) {
						buffer.get(current).refbit = false;
					}
					else {
						return buffer.get(current);
					}
				}
					
			}
			 current = (current+1) % buffer.size();
		 }
		return null;
	}
}
