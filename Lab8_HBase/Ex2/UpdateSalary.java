
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdateSalary
{

	private static final String CTE_TABLE_NAME = "employees";
	private static final String CTE_PROFESSIONAL = "professional_data";

	@SuppressWarnings("deprecation")
	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(CTE_TABLE_NAME));

			if (admin.tableExists(table.getTableName())) {

				HTable currentTable = new HTable(config,
						TableName.valueOf(CTE_TABLE_NAME));

				List<Put> list = new ArrayList<Put>();

				System.out.println("Updating John's Salary...");

				Put p16 = new Put(Bytes.toBytes("line1"));
				p16.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Salary"),
						Bytes.toBytes("160,000"));
				currentTable.put(p16);

				System.out.println("John's upgrade done!");

				currentTable.put(list);
				currentTable.close();

		}
		}
	}
}