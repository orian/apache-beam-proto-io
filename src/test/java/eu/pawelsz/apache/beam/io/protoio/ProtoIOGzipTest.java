package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOGzipTest {
    BoundedSource<Data.RawItem> source;
    PipelineOptions opts;

    @Before
    public void setUp() {
        source = CompressedSource.from(ProtoIO.source(Data.RawItem.class,
                "src/test/java/eu/pawelsz/apache/beam/io/protoio/test.pb.gz"));
    }

    @Test
    public void testEstimatedSizeBytes() throws Exception {
        assertEquals(199, source.getEstimatedSizeBytes(opts));
    }

    public static List<Data.RawItem> ALL = create();

    public static Data.RawItem RAW0 = Data.RawItem.newBuilder()
            .setTimestampUsec(1462100462000000L)
            .setDeviceName("device-0")
            .setMacAddress(ByteString.copyFromUtf8("\020\020\020\020\020\000"))
            .setSignalStrength(-30).build();

    private static List<Data.RawItem> create() {
        Data.RawItem rec = Data.RawItem.newBuilder()
                .setTimestampUsec(1462100462000000L)
                .setDeviceName("device-0")
                .setMacAddress(ByteString.copyFromUtf8("\020\020\020\020\020\000"))
                .setSignalStrength(-30).build();

        LinkedList<Data.RawItem> all = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            byte[] mac = rec.getMacAddress().toByteArray();
            mac[5] = (byte) i;
            all.add(rec.toBuilder()
                    .setTimestampUsec(rec.getTimestampUsec() + i * 3600000000L)
                    .setDeviceName("device-" + i).setSignalStrength(-30 - i)
                    .setMacAddress(ByteString.copyFrom(mac)).build());
        }
        return all;
    }

    @Test
    public void testReadRecords() throws IOException {
        BoundedSource.BoundedReader<Data.RawItem> reader =
                (BoundedSource.BoundedReader<Data.RawItem>) source.createReader(opts);
        assertTrue("must read 0", reader.start()); // start reading
        for (int i = 0; i < 9; i++) {
            assertEquals(ALL.get(i), reader.getCurrent());
            assertTrue("must read " + i, reader.advance());
        }

        assertEquals(ALL.get(9), reader.getCurrent());
        assertFalse("no more records", reader.advance());
        reader.close();
    }

    @Test
    public void testSerializable() {
        SerializableUtils.serializeToByteArray(source);
    }
}
