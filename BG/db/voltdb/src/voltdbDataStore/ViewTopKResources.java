package voltdbDataStore;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
@ProcInfo(                                                 
	    singlePartition = true,
	    partitionInfo = "Resource.wallUserId: 0"
	)
public class ViewTopKResources extends VoltProcedure {

	  public final SQLStmt sql = new SQLStmt(
		      "SELECT * FROM Resource WHERE wallUserId = ? ORDER BY rid LIMIT  ? ;"
				
		  );

	  public VoltTable[] run(int id,int rownum)
	      throws VoltAbortException {
	          voltQueueSQL( sql,id,rownum);
	          return voltExecuteSQL();
	      }

}
