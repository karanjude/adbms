package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UserOffset extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt("SELECT min(userid) FROM Users;");

	public VoltTable[] run() throws VoltAbortException {
		System.out.println("User Offset");
		voltQueueSQL(sql);
		return voltExecuteSQL();
	}

}
