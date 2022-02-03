import java.time.Instant;

/**
 * Represents a row in the parquet file.
 * This is hand coded based on the structure reported by:
 * try (org.apache.parquet.hadoop.ParquetFileReader	parquetFileReader=ParquetFileReader.open(new LocalInputFile(path))) {
 *   org.apache.parquet.schema.MessageType messageType=parquetFileReader.getFooter().getFileMetaData().getSchema();
 *   System.out.println(messageType.toString());
 * }
 */
public class WITRecord {
	public Instant instant; // time
	public double water;
	public double wet;
	public double bs;
	public double pv;
	public double npv;
	public String geometryWKT; // geometry
	public String xxxUID;

	public String toString() {
		return "{\"time\": \""+instant+"\", \"water\": "+water+", \"wet\": "+wet+", \"bs\": "+bs+", \"pv\": "+pv+", \"npv\": "+npv+", \"geometry\": \""+geometryWKT+"\", \"XXX_UID\": \""+xxxUID+"\"}";
	}
}
