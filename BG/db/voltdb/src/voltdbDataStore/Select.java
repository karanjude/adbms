package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Select extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt(
			"SELECT userid,dob FROM Users where userid=?;");

	public VoltTable[] run(int id) throws VoltAbortException {
		System.out.println("Select");
		voltQueueSQL(sql, EXPECT_NON_EMPTY, id);
		return voltExecuteSQL();
	}
}