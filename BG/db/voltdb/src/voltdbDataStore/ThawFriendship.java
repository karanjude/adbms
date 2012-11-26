package voltdbDataStore;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(

)
public class ThawFriendship extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"DELETE FROM friendship WHERE (userid1=? and userid2= ?) OR (userid1=? and userid2= ?) and status=2;");
	public final SQLStmt sql2 = new SQLStmt(
			"UPDATE Users SET confirmedFriends = confirmedFriends - 1 WHERE userid= ? OR userid=?;");

	public VoltTable[] run(int friendid1, int friendid2, int friendid22,
			int friendid21) throws VoltAbortException {
		System.out.println("Thaw Friendship");
		voltQueueSQL(sql, friendid1, friendid2, friendid22, friendid21);
		voltQueueSQL(sql2, friendid1, friendid22);
		return voltExecuteSQL();
	}

}
