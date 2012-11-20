package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertResources extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		      "INSERT INTO Resource VALUES (?, ?, ?, ?, ?, ?);"
		  );
	public final SQLStmt sql2 = new SQLStmt(
		      "UPDATE Users SET resourceCount = resourceCount+1 WHERE userid= ?;"
		  );

		  public VoltTable[] run( int rid,
		                          int creatorid ,
		                          int wallUsrId,
		                          String type,
		                          String body,
		                          String doc
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql, rid, creatorid, wallUsrId,type,body,doc);
		          voltQueueSQL( sql2, creatorid);
		          voltExecuteSQL();
		          return null;
		      }

}
