package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetCreatedResources extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"SELECT * FROM Resource WHERE creatorid = ?;");

	public VoltTable[] run(int id

	) throws VoltAbortException {
		System.out.println("Get Created Resources");
		voltQueueSQL(sql, id);
		return voltExecuteSQL();

	}

}
