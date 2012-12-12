package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

import org.voltdb.SQLStmt;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class ViewCommentOnResourceProc {

	public final String sql = new String("SELECT * FROM Modify WHERE rid = ?;");
	private final Connection conn;

	public ViewCommentOnResourceProc(Connection conn) {
		this.conn = conn;
	}

	public int execute(int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		// get comment cnt
		try {
			preparedStatement = conn.prepareStatement(sql);
			// preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, resourceID);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name, new StringByteIterator(value));
				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}
}
