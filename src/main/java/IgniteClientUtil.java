import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class IgniteClientUtil {

    private static final String DATA_CACHE = "DATA";
    private static final String IGNITE_SERVER = "192.168.0.11:10800";            // ip & port of ignite server

    private IgniteClient igniteClient = null;
    private ClientCache<Long, Record> recordCache = null;

    //--------------------------------------------------------------

    public void initIgniteClient() {

        try {
            // Starting the node
            ClientConfiguration cfg = new ClientConfiguration().setAddresses(IGNITE_SERVER);
            this.igniteClient = Ignition.startClient(cfg);

            ClientCacheConfiguration clientCacheConfiguration = new ClientCacheConfiguration();
            clientCacheConfiguration.setSqlSchema("PUBLIC");
            clientCacheConfiguration.setName(DATA_CACHE);
            this.recordCache = igniteClient.getOrCreateCache(clientCacheConfiguration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //--------------------------------------------------------------

    public void closeIgniteClient() {
        try {
            igniteClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //--------------------------------------------------------------

    public void insert(Record record) {

        // Insert persons.
        SqlFieldsQuery qry = new SqlFieldsQuery("insert into PUBLIC.DATA (id, parent_id, data, dt, url) values (?, ?, ?, ?, ?)");

        this.recordCache.query(qry.setArgs(record.id, record.parent_id, record.data, record.date, record.url)).getAll();
    }

    //--------------------------------------------------------------

}
