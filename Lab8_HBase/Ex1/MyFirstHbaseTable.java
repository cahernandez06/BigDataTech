
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	private static final String CTE_TABLE_NAME = "employees";
	private static final String CTE_ROWKEY = "row_key";
	private static final String CTE_PERSONAL = "personal_data";
	private static final String CTE_PROFESSIONAL = "professional_data";

	@SuppressWarnings("deprecation")
	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(CTE_TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CTE_ROWKEY));
			table.addFamily(new HColumnDescriptor(CTE_PERSONAL)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CTE_PROFESSIONAL));

			System.out.print("Creating employees table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println("Table employees created!");

			System.out.println("Populating the employees table...");

			HTable currentTable = new HTable(config,
					TableName.valueOf(CTE_TABLE_NAME));

			List<Put> list = new ArrayList<Put>();

			Put p1 = new Put(Bytes.toBytes("line1"));
			p1.add(CTE_ROWKEY.getBytes(), Bytes.toBytes("Empid"),Bytes.toBytes("1"));
			list.add(p1);
			Put p2 = new Put(Bytes.toBytes("line2"));
			p2.add(CTE_ROWKEY.getBytes(), Bytes.toBytes("Empid"),Bytes.toBytes("2"));
			list.add(p2);
			Put p3 = new Put(Bytes.toBytes("line3"));
			p3.add(CTE_ROWKEY.getBytes(), Bytes.toBytes("Empid"),Bytes.toBytes("3"));
			list.add(p3);

			Put p4 = new Put(Bytes.toBytes("line1"));
			p4.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("Name"),Bytes.toBytes("John"));
			list.add(p4);
			Put p5 = new Put(Bytes.toBytes("line2"));
			p5.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("Name"),Bytes.toBytes("Mary"));
			list.add(p5);
			Put p6 = new Put(Bytes.toBytes("line3"));
			p6.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("Name"),Bytes.toBytes("Bob"));
			list.add(p6);

			Put p7 = new Put(Bytes.toBytes("line1"));
			p7.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("City"),Bytes.toBytes("Boston"));
			list.add(p7);
			Put p8 = new Put(Bytes.toBytes("line2"));
			p8.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("City"),Bytes.toBytes("New York"));
			list.add(p8);
			Put p9 = new Put(Bytes.toBytes("line3"));
			p9.add(CTE_PERSONAL.getBytes(), Bytes.toBytes("City"),Bytes.toBytes("Fremont"));
			list.add(p9);

			Put p10 = new Put(Bytes.toBytes("line1"));
			p10.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Designation"),Bytes.toBytes("Manager"));
			list.add(p10);
			Put p11 = new Put(Bytes.toBytes("line2"));
			p11.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Designation"),Bytes.toBytes("Sr. Engineer"));
			list.add(p11);
			Put p12 = new Put(Bytes.toBytes("line3"));
			p12.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Designation"),Bytes.toBytes("Jr. Engineer"));
			list.add(p12);

			Put p13 = new Put(Bytes.toBytes("line1"));
			p13.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Salary"),Bytes.toBytes("150,000"));
			list.add(p13);
			Put p14 = new Put(Bytes.toBytes("line2"));
			p14.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Salary"),Bytes.toBytes("130,000"));
			list.add(p14);
			Put p15 = new Put(Bytes.toBytes("line3"));
			p15.add(CTE_PROFESSIONAL.getBytes(), Bytes.toBytes("Salary"),Bytes.toBytes("90,000"));
			list.add(p15);

			currentTable.put(list);
			currentTable.close();

			System.out.println("Table employees populated!");

		}
	}
}