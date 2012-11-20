package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class FriendsPerUser extends VoltProcedure {

	  public final SQLStmt sql = new SQLStmt(
	      "SELECT confirmedFriends FROM Users where userid = ?;"
	  );

	  public VoltTable[] run(int id)
	      throws VoltAbortException {
	          voltQueueSQL( sql,id);
	          return voltExecuteSQL();
	      }

}
