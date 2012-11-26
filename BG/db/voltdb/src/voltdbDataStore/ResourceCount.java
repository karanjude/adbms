package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ResourceCount extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt(
			"SELECT resourceCount FROM Users  where userid = ?;");

	public VoltTable[] run(int id) throws VoltAbortException {
		System.out.println("Resource Count");
		voltQueueSQL(sql, id);
		return voltExecuteSQL();
	}

}
