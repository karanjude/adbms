package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ProfileDetails extends VoltProcedure {

	public final SQLStmt sql = new SQLStmt(
			"SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  Users WHERE userid = ?;");

	public VoltTable[] run(int id) throws VoltAbortException {
		System.out.println("Profile Details");
		voltQueueSQL(sql, id);
		return voltExecuteSQL();
	}

}
