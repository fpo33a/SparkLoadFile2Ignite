import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Record class.
 */
public class Record implements Serializable {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Person ID . */
    @QuerySqlField
    public Long id;

    /** name (indexed). */
    @QuerySqlField(index = true)
    public String data;


    /** parent_Id */
    @QuerySqlField
    public Long parent_id;

    /** date. */
    @QuerySqlField
    public String date;

    /** url. */
    @QuerySqlField
    public String url;

    /**
     * Default constructor.
     */
    public Record() {
        // No-op.
    }

    /**
     * Constructs person record.
     *
     * @param p_id Id.
     * @param p_parent_id parent_Id.
     * @param p_data  name.
     * @param p_date  date/time.
     * @param p_url   url
     */
    public Record(Long p_id, Long p_parent_id, String p_data, String p_date, String p_url) {
        this.id = p_id;
        this.parent_id = p_parent_id;
        this.data = p_data;
        this.date = p_date;
        this.url = p_url;
    }



    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return "Record [id=" + id +
                ", parent_id=" + parent_id +
                ", data=" + data +
                ", date=" + date +
                ", url=" + url +']';
    }
}

