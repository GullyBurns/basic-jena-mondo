package org.czi.meta.utils;

import java.awt.Dimension;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.DimensionsRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import cern.colt.matrix.ObjectFactory2D;
import cern.colt.matrix.ObjectMatrix2D;

public class ExcelEngine implements HSSFListener {

	private boolean locationIndexingOn;	
	
	private HSSFWorkbook wb;
	private HSSFCellStyle cs;
	private HSSFFont f;

	private Map<String,ObjectMatrix2D> data = new HashMap<String,ObjectMatrix2D>();

	private Map<String,Map<String,String>> locations = new HashMap<String,Map<String,String>>();

	private HashMap lookupObjects = new HashMap();

	private short bStyle = HSSFCellStyle.BORDER_THIN;

	private short bStyle2 = HSSFCellStyle.BORDER_MEDIUM;

	private List<String> sheets = new ArrayList<String>();

	private String currSheet;

	private int sheetnum;

	private SSTRecord sstrec;

	public ExcelEngine() {
		super();
		this.locationIndexingOn = true;
	}

	public ExcelEngine(boolean locIdx) {
		super();
		this.locationIndexingOn = locIdx;
	}
	
	public void processRecord(Record record) {

		switch (record.getSid()) {
		// the BOFRecord can represent either the beginning of a sheet or the
		// workbook
		case BOFRecord.sid:
			
			BOFRecord bof = (BOFRecord) record;
			if (bof.getType() == bof.TYPE_WORKSHEET) {
				this.currSheet = (String) this.sheets.get(getSheetnum());
				setSheetnum(getSheetnum() + 1);
			}
			break;

		case DimensionsRecord.sid:
			
			DimensionsRecord dimr = (DimensionsRecord) record;

		    ObjectFactory2D F = ObjectFactory2D.dense;
		    ObjectMatrix2D  mat = F.make(dimr.getLastRow(), 
		    		dimr.getLastCol(), 
		    		"");
			
			this.getData().put(this.currSheet, mat);
			
			if(this.locationIndexingOn) {
				this.getLocations().put(this.currSheet, new HashMap<String, String>());
			}

			//this.getLocations().put(bsr.getSheetname(), new HashMap<String, String>());
			break;

		case BoundSheetRecord.sid:
		
			BoundSheetRecord bsr = (BoundSheetRecord) record;
			this.sheets.add(bsr.getSheetname());
			currSheet = bsr.getSheetname();
			
			break;
		
		case RowRecord.sid:
			
			RowRecord rowrec = (RowRecord) record;
			break;

		case NumberRecord.sid:
			
			NumberRecord numrec = (NumberRecord) record;

			String contents = "" + numrec.getValue();
			String ref = "R" + numrec.getRow() + "C" + numrec.getColumn();

			ObjectMatrix2D d = this.getData().get(this.currSheet);

			d.set(numrec.getRow(), numrec.getColumn(), contents);

			if(this.locationIndexingOn) {
				Map<String,String> l = this.getLocations().get(this.currSheet);
				if (l.keySet().contains(contents)) {
					String temp = (String) l.get(contents);
					l.put(contents, temp + "," + ref);
				} else {
					l.put(contents, ref);
				}
			}
			
			break;
			
		case SSTRecord.sid:
			
			sstrec = (SSTRecord) record;

			break;
			
		case LabelSSTRecord.sid:
			
			LabelSSTRecord lrec = (LabelSSTRecord) record;
			contents = "" + sstrec.getString(lrec.getSSTIndex());
			ref = "R" + lrec.getRow() + "C" + lrec.getColumn();

			d = this.getData().get(this.currSheet);

			d.set(lrec.getRow(), lrec.getColumn(), contents);

			if(this.locationIndexingOn) {
				Map<String,String> l = this.getLocations().get(this.currSheet);
				if (l.keySet().contains(contents)) {
					String temp = (String) l.get(contents);
					l.put(contents, temp + "," + ref);
				} else {
					l.put(contents, ref);
				}
			}		
			break;
			
		}

	}

	
	public void readByteArray(byte[] data) throws IOException {
		
		ByteArrayInputStream bis = new ByteArrayInputStream(data);

		POIFSFileSystem poifs = new POIFSFileSystem(bis);
		
		InputStream din = poifs.createDocumentInputStream("Workbook");

		HSSFRequest req = new HSSFRequest();

		req.addListenerForAllRecords(this);

		HSSFEventFactory factory = new HSSFEventFactory();

		factory.processEvents(req, din);

		bis.close();

		din.close();

	}
	
	public void readFile(File f) throws IOException {

		if( !f.getName().endsWith(".xls") ) 
			return;
		
		FileInputStream fin = new FileInputStream(f);

		POIFSFileSystem poifs = new POIFSFileSystem(fin);
		
		InputStream din = poifs.createDocumentInputStream("Workbook");

		HSSFRequest req = new HSSFRequest();

		req.addListenerForAllRecords(this);

		HSSFEventFactory factory = new HSSFEventFactory();

		factory.processEvents(req, din);

		fin.close();

		din.close();

	}

	public void readDirectoryOfTabFiles(File d) throws IOException {
		
		//
		// you run this function, you're not getting any indexing!
		//
		this.locationIndexingOn = false;
		
		File[] files = d.listFiles();
		for(int i=0; i<files.length; i++) {
			File f = files[i];

			String sName = f.getName().replaceAll("\\.txt", "");

		    ObjectMatrix2D  mat = this.readTabFile(f);
		    			
			this.data.put(sName, mat);
		}
		
	}
	
	public ObjectMatrix2D readTabFile(File f) throws IOException {
		
		String sName = f.getName().replaceAll("\\.txt", "");

		// need to run through each file twice, 
		// once to count the lines and then to build the matrix
		BufferedReader br = new BufferedReader(
				new InputStreamReader(new FileInputStream(f))
				);
		
		String line = br.readLine();
		int rCount = 1;
		int cCount = line.length() - line.replaceAll("\\t","").length() + 1;
		while((line = br.readLine()) != null)
	        rCount++;
		br.close();

		ObjectFactory2D F = ObjectFactory2D.dense;
	    ObjectMatrix2D  mat = F.make(rCount, cCount, "");

		//
	    // poor man's 'split' function.
		// trying to avoid nasty memory leaks.
	    //
	    FileReader fr = new FileReader(f);
	    int row = 0, col = 0;
	    int charCode;
	    StringBuffer sb = new StringBuffer();
		while((charCode = fr.read()) != -1) {
			char ch = (char) charCode;
			
			if( ch == '\t' ) {
				
				String cell = sb.toString();
				if(cell.length() > 0) {
					mat.set(row, col, cell);
					sb.delete(0, sb.length());
					col++;
				}
				
			} else if( ch == '\r' || ch == '\n' ) {
				
				String cell = sb.toString();
				if(cell.length() > 0) {
					mat.set(row, col, cell);
					sb.delete(0, sb.length());
					row++;
					col = 0;	
				}
				
			} else {
				sb.append(ch);
			}
			
		}
		fr.close();
		
		this.data.put(sName, mat);
	
		return mat;
		
	}
		
	
	public List<Ref> getRefs(String rcStr) throws Exception {

		List<Ref> refs = new ArrayList<Ref>();

		String[] rcArray = rcStr.split(",");
		for(int i=0; i<rcArray.length; i++) {
		
			Ref ref = null;
			Pattern p = Pattern.compile("R(\\d+)C(\\d+)");
			Matcher m = p.matcher(rcArray[i]);
	
			if (m.find()) {
				int r = (new Integer(m.group(1))).intValue();
				int c = (new Integer(m.group(2))).intValue();
	
				ref = new Ref(r, c);
				refs.add(ref);
			}
		
		}
		
		return refs;

	}

	public Ref getRef(String rcStr) throws Exception {

		Ref ref = null;

		Pattern p = Pattern.compile("R(\\d+)C(\\d+)");
		Matcher m = p.matcher(rcStr);

		if (m.find()) {
			int r = (new Integer(m.group(1))).intValue();
			int c = (new Integer(m.group(2))).intValue();

			ref = new Ref(r, c);
		}
		return ref;

	}

	public Dimension getMatrixDimensions(String sheetName) throws Exception {

		ObjectMatrix2D mat = this.getData().get(sheetName);

		Dimension dim = new Dimension(mat.columns(), mat.rows());

		return dim;

	}

	
	public String getData(int r, int c, String sheetName) throws Exception {

		ObjectMatrix2D mat = this.getData().get(sheetName);

		String dat = (String) mat.get(r, c);

		return dat;

	}
	
	public ObjectMatrix2D getMatrixForSheet(String sheetName) throws Exception {
		
		return this.getData().get(sheetName);

	}
	
	public class Ref {

		private int r;

		private int c;

		public Ref(int r, int c) {
			this.r = r;
			this.c = c;
		}
		
		public int getR() {
			return this.r;
		}
		
		public int getC() {
			return this.c;
		}
		
	}

	public HashMap getLookupObjects() {

		return lookupObjects;
	}

	public Map<String,ObjectMatrix2D> getData() {
		return data;
	}

	public Map<String,Map<String,String>> getLocations() {
		return locations;
	}

	protected HSSFWorkbook getWb() {
		return wb;
	}

	protected void setWb(HSSFWorkbook wb) {
		this.wb = wb;
	}

	protected int getSheetnum() {
		return sheetnum;
	}

	protected void setSheetnum(int sheetnum) {
		this.sheetnum = sheetnum;
	}

	protected HSSFCellStyle getCs() {
		return cs;
	}

	protected void setCs(HSSFCellStyle cs) {
		this.cs = cs;
	}

	protected HSSFFont getF() {
		return f;
	}

	protected void setF(HSSFFont f) {
		this.f = f;
	}

	public boolean isLocationIndexingOn() {
		return locationIndexingOn;
	}

	public Map<String, Integer> getColumnHeadings(String s) throws Exception {
		Map<String, Integer> columnHeadings = new HashMap<String, Integer>();

        Dimension d = this.getMatrixDimensions(s);

		LOOP: for(int i = 0; i < 20 && i < d.width; i++) {
        	String v = this.getData(0, i, s);
        	
        	// strip trailing spaces in the excel cell
        	v = v.replaceAll("[ ]+$", "");
        	
        	if( v == null || v.length() == 0 )
        		break LOOP;
        	columnHeadings.put( v, i);
        }
		
		return columnHeadings;
	}
	
	public Map<String, Integer> getRowHeadings(String s) throws Exception {
		
        Dimension d = this.getMatrixDimensions(s);

		Map<String, Integer> rowHeadings = new HashMap<String, Integer>();

		LOOP: for(int i = 0; i < 20 && i < d.height; i++) {
        	String v = this.getData(i, 0, s);
        	
        	// strip trailing spaces in the excel cell
        	v = v.replaceAll("[ ]+$", "");
        	
        	if( v == null || v.length() == 0 )
        		break LOOP;
        	rowHeadings.put( v, i);
        }
		
		return rowHeadings;
	}
	
	
}
