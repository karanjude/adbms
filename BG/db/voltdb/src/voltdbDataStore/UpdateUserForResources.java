package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UpdateUserForResources extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		      "UPDATE Users SET resourceCount = resourceCount +1 WHERE userid= ?;"
		  );

		  public VoltTable[] run( int uId
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql, uId);
		          voltExecuteSQL();
		          return null;
		      }
}
