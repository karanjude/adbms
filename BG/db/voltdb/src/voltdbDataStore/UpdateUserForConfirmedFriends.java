package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UpdateUserForConfirmedFriends extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public VoltTable[] run(int uId) throws VoltAbortException {
		System.out.println("Update User For Confirmed Friendship");
		voltQueueSQL(sql, uId);
		voltExecuteSQL();
		return null;
	}

}
