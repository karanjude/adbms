package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ListFriendsPic extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt(
			"SELECT userid, userid1, userid2, username, fname,"
					+ " lname, gender, dob, jdate, ldate, address,email,tel,tpic"
					+ " FROM users, friendship WHERE  (userid1=? and userid=userid2) and status = 2");

	public VoltTable[] run(int id) throws VoltAbortException {
		System.out.println("List Friends Pic");
		voltQueueSQL(sql, id);
		return voltExecuteSQL();
	}

}
