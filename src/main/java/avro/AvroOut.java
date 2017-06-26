package avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.*;

/**
 * Created by ggchangan on 17-6-24.
 */
public class AvroOut {
    public static void main(String[] args) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        InputStream avroInputStream = AvroOut.class.getClassLoader().getResourceAsStream("StringPair.avsc");

        assert avroInputStream != null;

        Schema schema = parser.parse(avroInputStream);

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", new Utf8("L"));
        datum.put("right", new Utf8("R"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        //TODO test other encoder
        //Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(datum, encoder);
        encoder.flush();

        outputStream.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);

        assert result.get("left").toString().equalsIgnoreCase("L");
        assert result.get("right").toString().equalsIgnoreCase("R");
    }
}
