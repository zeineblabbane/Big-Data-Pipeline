import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class hbasefootball {

    private Table table1;
    private String tableName = "football";
    private String family1 = "footballteam";
    private String family2 = "footballaveragegoals";

    public void createHbaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        ht.addFamily(new HColumnDescriptor(family1));
        ht.addFamily(new HColumnDescriptor(family2));
        System.out.println("connecting");

        System.out.println("Creating Table");
        createOrOverwrite(admin, ht);
        System.out.println("Done......");

        table1 = connection.getTable(TableName.valueOf(tableName));
        try {
            File myObj = new File("football-after-kafka");
            Scanner myReader = new Scanner(myObj);
            int i = 0;
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] parts = data.split("\t");
                String part1 = parts[0];
                String part2 = parts[1];
                i=i+1;
                String s = "footballteam" + String.valueOf(i);
                byte[] row = Bytes.toBytes(part1);
                Put p = new Put(row);
                p.addColumn(family1.getBytes(), "footballname".getBytes(), Bytes.toBytes(part1));
                p.addColumn(family2.getBytes(), "goals".getBytes(), Bytes.toBytes(part2));
                table1.put(p);
            }
            myReader.close();
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        finally {
            table1.close();
            connection.close();
        }
    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void main(String[] args) throws IOException {
        hbasefootball admin = new hbasefootball();
        admin.createHbaseTable();
    }
}
