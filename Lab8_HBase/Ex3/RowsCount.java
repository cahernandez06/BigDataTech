
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

public class RowsCount
{

	private static final String CTE_TABLE_NAME = "employees";
//	private static final String CTE_ROWKEY = "row_key";
//	private static final String CTE_PERSONAL = "personal_data";
//	private static final String CTE_PROFESSIONAL = "professional_data";

	@SuppressWarnings("deprecation")
	public static void main(String... args) throws IOException {
		int count = 0;

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
			 Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(CTE_TABLE_NAME));

			if (admin.tableExists(table.getTableName())) {
				
				System.out.println("Starting rows count...");
				
				Scan scan = new Scan();
				HTable currentTable = new HTable(config,TableName.valueOf(CTE_TABLE_NAME));
				FilterList allFilters = new FilterList();
				allFilters.addFilter(new FirstKeyOnlyFilter());
				allFilters.addFilter(new KeyOnlyFilter());
				scan.setFilter(allFilters);
				ResultScanner scanner = currentTable.getScanner(scan);

				for (Result rs = scanner.next(); rs != null; rs = scanner.next()) 
					count++;

				System.out.println("Rows count: " + count);
				System.out.println("Count Rows finished!");

				currentTable.close();

			}

		}
	}
}