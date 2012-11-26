package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertPendingFriends extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"INSERT INTO Friendship VALUES (?, ?, ?);");

	public final SQLStmt sql2 = new SQLStmt(
			"UPDATE Users SET pendingFriends = pendingFriends +1 WHERE userid= ?;");

	public VoltTable[] run(int friendid1, int friendid2, int status

	) throws VoltAbortException {
		System.out.println("Insert Pending Friends");
		voltQueueSQL(sql, friendid1, friendid2, status);
		voltQueueSQL(sql2, friendid1);
		voltExecuteSQL();
		return null;
	}

}
