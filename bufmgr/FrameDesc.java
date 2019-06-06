package bufmgr;
import global.*;

public class FrameDesc extends Page {

	 protected PageId pageno; // disk page number
	 protected boolean dirty; 
	 protected int pincount; // to count the number of users
	 protected boolean valid; //valid data in frame
	 protected boolean refbit; //reference count
	 
	 public FrameDesc() {
		 
		 pageno = new PageId();
		 dirty = false;
		 pincount = 0;
		 valid = false;
		 refbit = false;

	}
}
