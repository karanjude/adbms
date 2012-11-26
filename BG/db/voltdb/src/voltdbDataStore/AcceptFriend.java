package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class AcceptFriend extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt(
			"UPDATE Friendship SET status = 2 WHERE userid1=? and userid2= ?;");
	public final SQLStmt sql2 = new SQLStmt(
			"UPDATE Users SET pendingFriends = pendingFriends - 1 WHERE userid= ?;");
	public final SQLStmt sql3 = new SQLStmt(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public final SQLStmt sql4 = new SQLStmt(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public VoltTable[] run(int inviter, int invitee) throws VoltAbortException {
		System.out.println("Accept Friend");
		voltQueueSQL(sql, inviter, invitee);
		voltQueueSQL(sql2, inviter);
		voltQueueSQL(sql3, inviter);
		voltQueueSQL(sql4, invitee);
		voltExecuteSQL();
		return null;
	}

}
