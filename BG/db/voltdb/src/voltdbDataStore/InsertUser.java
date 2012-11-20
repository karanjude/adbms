package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertUser extends VoltProcedure  {
	public final SQLStmt sql = new SQLStmt(
		      "INSERT INTO Users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?);"
		  );

		  public VoltTable[] run( int uId,
		                          String uName ,
		                          String pwd,
		                          String fname,
		                          String lname,
		                          String gender,
		                          String dob,
		                          String jdate,
		                          String ldate,
		                          String address,
		                          String email,
		                          String tel,
		                          int confirmedFriends,
		                          int pendingFrineds,
		                          int resourceCount
		                          
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql, uId, uName, pwd,fname,lname,gender,dob,jdate,ldate,address,email,tel,confirmedFriends,pendingFrineds,resourceCount);
		          voltExecuteSQL();
		          return null;
		      }
}
