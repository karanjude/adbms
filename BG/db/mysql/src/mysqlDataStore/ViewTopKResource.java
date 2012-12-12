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

public class ViewTopKResource {

	private final Connection conn;

	public final String sql = new String(
			"SELECT * FROM Resource WHERE wallUserId = ? ORDER BY rid LIMIT  ? ;");

	public ViewTopKResource(Connection conn) {
		this.conn = conn;
	}

	public int execute(int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		int retVal = 0;
		try {
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k + 1));
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name.toUpperCase(), new StringByteIterator(
							value));
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
