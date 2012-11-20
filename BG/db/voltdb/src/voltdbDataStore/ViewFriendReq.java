package voltdbDataStore;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
@ProcInfo(
	    
	)
public class ViewFriendReq extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			 "SELECT userid, userid1, userid2, username, fname, lname,gender, dob, jdate, ldate, address,email,tel "+ 
					 "FROM Users, Friendship WHERE (userid2=? and status = 1 and userid1 = userid)" +
					 "order by userid, userid1, userid2, username, fname, lname, gender, dob, jdate, ldate, address,email,tel;" 
					 		  );

		  public VoltTable[] run(int id)
	      throws VoltAbortException {
				voltQueueSQL( sql,id);
				return voltExecuteSQL();
}


}
