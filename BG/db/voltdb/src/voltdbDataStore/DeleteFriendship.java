package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class DeleteFriendship extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		      "DELETE FROM Friendship WHERE userid1=? and userid2= ? and status=1;"
		  );

	
		  public VoltTable[] run( int friendid1,
		                          int friendid2
		                        
		                         
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql,friendid1,friendid2);
		          voltExecuteSQL();
		          return null;
		      }


}
