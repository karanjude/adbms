package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class SelectUserCount extends VoltProcedure {

	  public final SQLStmt sql = new SQLStmt(
	      "SELECT count(*) FROM Users;"
	  );

	  public VoltTable[] run()
	      throws VoltAbortException {
	          voltQueueSQL( sql);
	          return voltExecuteSQL();
	      }

}
