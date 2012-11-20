package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

public class InsertFriends extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		      "INSERT INTO Friendship VALUES (?, ?, ?);"
		  );

	public final SQLStmt sql2 = new SQLStmt(
		      "UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;"
		  );
	
		  public VoltTable[] run( int friendid1,
		                          int friendid2,
		                          int status
		                         
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql,friendid1,friendid2,status);
		          voltQueueSQL( sql2,friendid1);
		          voltQueueSQL( sql,friendid2,friendid1,status);
		          voltQueueSQL( sql2,friendid2);
		          voltExecuteSQL();
		          return null;
		      }


}
