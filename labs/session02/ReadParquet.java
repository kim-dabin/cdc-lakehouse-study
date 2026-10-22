// Read selected physical objects; never write S3, tables, catalog or offsets.
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReadParquet {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) throw new IllegalArgumentException("Metadata, target PK and files required");
    ObjectMapper json = new ObjectMapper();
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.endpoint", "http://minio:9000");
    properties.put("s3.path-style-access", "true");
    properties.put("client.region", "us-east-1");
    S3FileIO io = new S3FileIO();
    io.initialize(properties); // Uses the already configured container AWS environment.
    try {
      Schema schema = TableMetadataParser.read(io, args[0]).schema();
      List<Map<String, Object>> files = new ArrayList<>();
      for (int index = 2; index < args.length; index++) {
        String path = args[index];
        if (!path.matches("s3://warehouse/bronze/cdc_events/data/[A-Za-z0-9_./=-]+\\.parquet"))
          throw new IllegalArgumentException("Unexpected data file path");
        InputFile input = io.newInputFile(path);
        if (input.getLength() > 16 * 1024 * 1024) throw new IllegalArgumentException("Teaching file exceeds 16 MiB safety bound");
        byte[] bytes;
        try (InputStream stream = input.newStream()) { bytes = stream.readAllBytes(); }
        if (bytes.length < 8) throw new IllegalArgumentException("Truncated file");
        String start = new String(bytes, 0, 4, StandardCharsets.US_ASCII);
        String end = new String(bytes, bytes.length - 4, 4, StandardCharsets.US_ASCII);
        if (!start.equals("PAR1") || !end.equals("PAR1")) throw new IllegalArgumentException("Missing PAR1 file magic");
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        List<Map<String, Object>> matches = new ArrayList<>();
        long count = 0;
        try (CloseableIterable<Record> records = Parquet.read(input).project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema)).build()) {
          for (Record record : records) {
            count++;
            String payload = String.valueOf(record.getField("payload"));
            JsonNode envelope = json.readTree(payload);
            if (!args[1].equals(envelope.path("before").path("refund_id").asText()) &&
                !args[1].equals(envelope.path("after").path("refund_id").asText())) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            for (String name : List.of("topic", "partition_id", "kafka_offset", "payload", "kafka_ts", "ingested_at")) {
              Object value = record.getField(name);
              row.put(name, value == null || value instanceof Number || value instanceof Boolean ? value : value.toString());
            }
            matches.add(row);
          }
        }
        Map<String, Object> file = new LinkedHashMap<>();
        file.put("file_path", path); file.put("byte_count", bytes.length);
        file.put("sha256", hash); file.put("magic_start", start); file.put("magic_end", end);
        file.put("decoded_record_count", count); file.put("matching_records", matches);
        files.add(file);
      }
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("read_method", "S3FileIO physical bytes + Iceberg GenericParquetReaders; no table scan substitution");
      result.put("physical_files", files);
      System.out.println(json.writeValueAsString(result));
    } finally { io.close(); }
  }
}
