package voltdbDataStore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class PostCommentOnResource extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"INSERT INTO Modify VALUES (?, ?, ?, ?, ?, ?);");

	public VoltTable[] run(int creatorid, int rid, int modifierid,
			String timestamp, String type, String content

	) throws VoltAbortException {
		System.out.println("Post Comment On Resource");
		voltQueueSQL(sql, creatorid, rid, modifierid, timestamp, type, content);
		voltExecuteSQL();
		return null;
	}
}
