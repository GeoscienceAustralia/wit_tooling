import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

// Java project to read a parquet file with minimum dependencies
// https://github.com/GeoscienceAustralia/wit_tooling/tree/main/examples/parquet/java/parquet-reader-without-avro
// Author https://github.com/MartinJericho
public class Main {
	private static final int MAX_RECORDS_TO_READ=3;

	public static void main(String[] args) {
		Path path=Paths.get("in/sampledata.parquet");
		// Read without Avro. Adapted from https://www.arm64.ca/post/reading-parquet-files-java/
		try (ParquetFileReader parquetFileReader=ParquetFileReader.open(new LocalInputFile(path))) {
			// Display the schema. Much simpler than uk.co.hadoopathome.intellij.viewer.fileformat.ParquetFileReader.getSchema()
			MessageType messageType=parquetFileReader.getFooter().getFileMetaData().getSchema();
			System.out.println(messageType);
			// Example of iterating through the schema fields:
			System.out.println("Iterating through schema fields:");
			List<Type> types=messageType.getFields();
			for (Type type : types) {
				PrimitiveType primitiveType=type.asPrimitiveType(); // Assume a flat structure, all PrimitiveType, no GroupType
				System.out.println(primitiveType.getPrimitiveTypeName()+" "+primitiveType.getName());
			}
			// Read data:
			System.out.println("\nReading some data:");
			readRecords(parquetFileReader,messageType,MAX_RECORDS_TO_READ);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	private static void readRecords(ParquetFileReader parquetFileReader, MessageType messageType, int maxRecordsToRead) throws IOException {
		int readCount=0;
		PageReadStore pageReadStore;
		while ((pageReadStore=parquetFileReader.readNextRowGroup())!=null) {
			long recordCount=pageReadStore.getRowCount();
			MessageColumnIO messageColumnIO=new ColumnIOFactory().getColumnIO(messageType);
			RecordReader<Group> recordReader=messageColumnIO.getRecordReader(pageReadStore,new GroupRecordConverter(messageType));
			for (int i=0; i<recordCount; i++) {
				if (readCount==maxRecordsToRead) {
					System.out.printf("Retrieved the first %d records%n",readCount);
					return;
				}
				Group group=recordReader.read();
				//System.out.println(group);
				WITRecord witRecord=new WITRecord();
				witRecord.instant=getInstant(group.getInt96(0,0));
				witRecord.water=group.getDouble(1,0);
				witRecord.wet=group.getDouble(2,0);
				witRecord.bs=group.getDouble(3,0);
				witRecord.pv=group.getDouble(4,0);
				witRecord.npv=group.getDouble(5,0);
				witRecord.geometryWKT=group.getString(6,0);
				witRecord.xxxUID=group.getString(7,0);
				System.out.println(witRecord);
				readCount++;
			}
		}
		System.out.printf("Retrieved all %d records%n",readCount);
	}

	private static Instant getInstant(Binary int96Binary) {
		NanoTime nanoTime=NanoTime.fromBinary(int96Binary);
		final long nanosInSecond = 1000 * 1000 * 1000;
		final long nanosecondsSinceUnixEpoch=(nanoTime.getJulianDay()-2440588) * (86400 * nanosInSecond) + nanoTime.getTimeOfDayNanos();
		return Instant.ofEpochSecond(nanosecondsSinceUnixEpoch / nanosInSecond, nanosecondsSinceUnixEpoch % nanosInSecond);
	}
}
