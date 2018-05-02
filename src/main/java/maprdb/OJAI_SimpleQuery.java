package maprdb;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;

//import java.util.UUID;

/*
 Java OJAI 
 Querys Payments table in MapR-DB JSON
 */
public class OJAI_SimpleQuery {

    public static final String OJAI_CONNECTION_URL = "ojai:mapr:";

    public static void main(String[] args) {
        //Full path including namespace /mapr/<cluster-name>/apps/
        String tableName = "/user/mapr/demo.mapr.com/tables/payments";
        if (args.length == 1) {
            tableName = args[0];

        } else {
            System.out.println("Using hard coded parameters unless you specify the file and topic. <file topic>   ");
        }

        System.out.println("==== Start Application ===");

        // Create an OJAI connection to MapR cluster
        Connection connection = DriverManager.getConnection(OJAI_CONNECTION_URL);
        // Get an instance of OJAI
        DocumentStore store = connection.getStore(tableName);
        System.out.println("find payments > $10,000");
        Query query = connection.newQuery()
                .select("_id", "nature_of_payment", "amount") // projection
                .where(connection.newCondition().is("amount", QueryCondition.Op.GREATER_OR_EQUAL, 10000).build()) // condition
                .build();

        long startTime = System.currentTimeMillis();
        int counter = 0;
        DocumentStream stream = store.findQuery(query);
        for (Document userDocument : stream) {
            // Print the OJAI Document
            System.out.println("\t" + userDocument.asJsonString());
            counter++;
        }
        long endTime = System.currentTimeMillis();

        System.out.println(String.format("\t %d found in %d ms", counter, (endTime - startTime)));

        System.out.println("find payments for february");

        query = connection.newQuery()
                .select("_id", "nature_of_payment", "amount") // projection
                .where(connection.newCondition().like("_id", "%[_]02/%").build()) // condition
                .build();

        startTime = System.currentTimeMillis();
        counter = 0;
        stream = store.findQuery(query);
        for (Document userDocument : stream) {
            // Print the OJAI Document
            System.out.println("\t" + userDocument.asJsonString());
            counter++;
        }
        endTime = System.currentTimeMillis();

        System.out.println(String.format("\t %d found in %d ms", counter, (endTime - startTime)));

        // Close this instance of OJAI DocumentStore
        store.close();

        // close the OJAI connection and release any resources held by the connection
        connection.close();

        System.out.println("==== End Application ===");
    }

}
