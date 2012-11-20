package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ViewCommentOnResource extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
		      "SELECT * FROM Modify WHERE rid = ?;"
		  );

	
		  public VoltTable[] run( int id
		                         
		                          )
		      throws VoltAbortException {
		          voltQueueSQL( sql,id);
		          return voltExecuteSQL();
		           
		      }


}
