package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ViewFriendReqPic extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"SELECT userid, userid1, userid2, username, fname, "
					+ "lname, gender, dob, jdate, ldate, address,email,tel,tpic"
					+ " FROM Users, Friendship WHERE userid2=? and status = 1 and userid1 = userid "
					+ "order by userid, userid1, userid2, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic;");

	public VoltTable[] run(int inviteeId) throws VoltAbortException {
		System.out.println("View Friend Req Pic");
		voltQueueSQL(sql, inviteeId);
		return voltExecuteSQL();
	}

}
