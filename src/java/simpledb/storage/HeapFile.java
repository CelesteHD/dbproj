package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */

public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File file;
    private final TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
	this.file = f;
	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];

	try
	{
        	// create a new  RandomAccessFile object  on `file` in read mode
		RandomAccessFile raf = new RandomAccessFile ( file, "r" );

        	// seek to `offset`
		raf.seek(offset);

       		// read pageSize bytes into `data`
		raf.read(data);

        	// close the file
		raf.close();

        	// return new HeapPage((HeapPageId) pid, data);
		return new HeapPage((HeapPageId) pid, data);
	}

	catch (IOException e)
	{
		throw new RuntimeException ("Failed to read page", e);
	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
	return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
    	private final TransactionId tid;
    	private int currentPageNo;
    	private Iterator<Tuple> tupleIterator;   // iterator over the CURRENT page's tuples

    	public HeapFileIterator(TransactionId tid) {
        	this.tid = tid;
    	}

    	public void open() throws DbException, TransactionAbortedException {
        	currentPageNo = 0;
        	tupleIterator = getTupleIteratorForPage(currentPageNo);
    	}

    	// return next tuple, or null when the whole table is done
    	protected Tuple readNext() throws DbException, TransactionAbortedException {
        	// if tupleIterator is null (not open), return null
		if (tupleIterator == null)
		{
			return null;
		}

		/* if page is empty, advance to next page
		*  if page number is more than actual available pages in table, indicates end of table
		*  returns null
		*  only then, run tupleIterator 
		*  after this while-loop, calling tupleIterator.next() is confirmed to find a page with tuple
		*/

		while (!tupleIterator.hasNext())
		{
			currentPageNo++;
			if (currentPageNo >= numPages())
			{
				return null;
			}
			tupleIterator = getTupleIteratorForPage(currentPageNo);

		}
		return tupleIterator.next();
	}

    	public void rewind() throws DbException, TransactionAbortedException {
        	close();
        	open();
    	}

	public void close() {
    		super.close();
    		tupleIterator = null;
    		currentPageNo = 0;
	}
    	// helper: load one page through the buffer pool and return its tuple iterator
    	private Iterator<Tuple> getTupleIteratorForPage(int pageNo)
           	throws DbException, TransactionAbortedException {
        	/*
			* - build a HeapPageId for (this table's id, pageNo)
        	* - call Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)
        	* - cast to HeapPage, return its .iterator() 
			*/
		HeapPageId hpid = new HeapPageId(getId(), pageNo);
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
		return page.iterator();
    		}
	}

}

