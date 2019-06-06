package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  // array list of frame descriptors named bufferpool	
  ArrayList<FrameDesc> bufferpool;
  
  // mapping of page id to frame descriptor using Hashmap
  HashMap<PageId, FrameDesc> mapPageFrame;
  
  Clock replPolicy;
  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {
  
    bufferpool = new ArrayList<FrameDesc>();
    
    for(int i=0;i<numframes;i++)
    {
    	bufferpool.add(new FrameDesc());
    }
	  
	mapPageFrame = new HashMap<PageId,FrameDesc>();
	replPolicy = new Clock(); 
	  
  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	  // to get the page in buffer pool
	  FrameDesc frameno = mapPageFrame.get(pageno);
	  
	  // if disk page pageno is already in the buffer pool, increment pincount
	  if(frameno != null) {
	        frameno.pincount++;
	  }
	  
	  // use replacement policy to select a frame for replacement
	  else
	  {
		  FrameDesc victim;
		  
		  // choose the current frame as victim
		  if(!bufferpool.isEmpty())
		  {  
			  victim = bufferpool.get(bufferpool.size()-1);
			  bufferpool.remove(victim);
		  }
		  
		  // use the replacement policy 
		  else 
		  {
			  victim = replPolicy.pickVictim(this);
			  
			  if(victim == null)
			  {
				  throw new IllegalStateException("Error: Pool is filled with all pages as Pinned");
			  }
			  else
			  {
				  if(victim.dirty)
				  {
					  // write the victim frame to disk
					  flushPage(pageno, victim); 
				  }
			  }
			  
			  // remove victim from buffer pool after writing to disk
			  mapPageFrame.remove(pageno); 
			  
			  // re-initialize frame values 
			  victim.pincount = 0;
			  victim.valid = false;
			  victim.dirty = false;
			  victim.refbit = false;
		  }
		  
		
		  if(contents == PIN_DISKIO) {
			// read the page from disk to the frame using read_page function
			  Minibase.DiskManager.read_page(pageno, victim);
				victim.pincount++;
				victim.valid = true;
				victim.dirty = false;
				victim.pageno = new PageId();
				victim.pageno.copyPageId(pageno);
				victim.refbit = true;
				
				mapPageFrame.put(victim.pageno, victim);
				mempage.setData(victim.getData());
				
		  }
		  else if(contents == PIN_MEMCPY) {
			// copy mempage into frame
			    victim.pincount++;
				victim.valid = true;
				victim.dirty = false;
				victim.pageno = new PageId();
				victim.pageno.copyPageId(pageno);
				victim.refbit = true;
				
				mapPageFrame.put(victim.pageno, victim);
				mempage.setPage(mempage);
				
		  }
		  else {
			// copy nothing into the frame
			    victim.pincount++;
				victim.valid = true;
				victim.dirty = false;
				victim.pageno = new PageId(pageno.pid);
				victim.pageno.copyPageId(pageno);
				victim.refbit = true;
				
				mapPageFrame.put(pageno, victim);
				
		  }
		  
	  }

  } // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

    FrameDesc frameno = mapPageFrame.get(pageno);
    
    if (frameno == null)
    {
    	throw new IllegalArgumentException("Page is not in the buffer pool");
    }
    else if (frameno.pincount == 0) 
    {
    	throw new IllegalArgumentException("Page is not pinned");
	}
    else 
    {
		if(dirty) {
			frameno.dirty = UNPIN_DIRTY;
		}
		else {
			frameno.dirty = UNPIN_CLEAN;
		}
		
		frameno.pincount--;
		
		if(frameno.pincount == 0)
		{
			frameno.refbit = true;
		}
		
	}

  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {

    PageId pageId = new PageId();
    FrameDesc frameDesc = mapPageFrame.get(pageId);
    if (frameDesc != null)
    {
    	if (frameDesc.pincount > 0) {
    	throw new IllegalArgumentException("First page is already pinned");
    }}
    else if (getNumUnpinned() == 0) {
    	throw new IllegalStateException("No unpinned pages");
	}
    else {
		pageId.pid = Minibase.DiskManager.allocate_page(run_size).pid;
		pinPage(pageId, firstpg, PIN_MEMCPY);
	}
    return pageId;
  } // public PageId newPage(Page firstpg, int run_size)

  
  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {
	  FrameDesc frameno = mapPageFrame.get(pageno);
	  
	    if (frameno != null)
	    {
	    	if (frameno.pincount > 0) 
	    	{
	    	throw new IllegalArgumentException("Page is in the buffer pool");
	    	}
	    }
	    else 
	    {
	    	//remove and deallocate the page
	    	if(mapPageFrame.containsKey(pageno.pid)) {
	    		mapPageFrame.remove(pageno);
	    	}
	    	Minibase.DiskManager.deallocate_page(pageno);
	    }
    

  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {
	  // write only the valid and dirty pages to disk
	  // remaining pages are flushed out
	  Iterator iterator = mapPageFrame.entrySet().iterator();
	    while (iterator.hasNext()) {                             
			Map.Entry map = (Map.Entry) iterator.next();
			PageId pageId = (PageId) map.getKey();
			FrameDesc frameDesc = (FrameDesc) map.getValue();
			iterator.remove();
			if ((frameDesc.valid && frameDesc.dirty) == true) {
				flushPage(pageId, frameDesc);
			}
		}
    

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno, FrameDesc frameno) {
	  
	if(frameno != null)
	{
		if(frameno.dirty == true) {
			// write the page to disk using write_page function
			Minibase.DiskManager.write_page(pageno, frameno);
		}
	}
	else
	{
		throw new IllegalArgumentException("Page is not in the buffer pool");
	}
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
    return mapPageFrame.size();
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    int unpinned = 0;              
    // If bufferpool is not empty then all the pages in the buffer pool are unpinned  
    if(!bufferpool.isEmpty()) {
    	unpinned = unpinned + bufferpool.size();
    }
    
    // If pin count in frameDesc is 0, then count towards unpinned buffer frames.
    Iterator iterator = mapPageFrame.entrySet().iterator();
    while (iterator.hasNext()) {                             
		Map.Entry map = (Map.Entry) iterator.next();
		PageId pageId = (PageId) map.getKey();
		FrameDesc frameDesc = (FrameDesc) map.getValue();
		if (frameDesc.pincount == 0) {
			unpinned++;
		}
	}
    return unpinned;
    
  }

} // public class BufMgr implements GlobalConst
