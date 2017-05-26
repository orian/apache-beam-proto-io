package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;
import eu.pawelsz.apache.beam.io.protoio.Data;

import java.io.IOException;

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

    @Test
    public void testReadRecords() throws IOException {
        BoundedSource.BoundedReader<Data.RawItem> reader =
                (BoundedSource.BoundedReader<Data.RawItem>) source.createReader(opts);
        assertTrue("must read 0", reader.start()); // start reading
        Data.RawItem itm = reader.getCurrent();
        assertEquals(1462100462000000L, itm.getTimestampUsec());
        assertEquals("device-0", itm.getDeviceName());
        assertEquals(-30, itm.getSignalStrength());
        assertTrue(itm.hasMacAddress());

        for (int i = 1; i < 10; i++) {
            assertTrue("must read " + i, reader.advance());
        }

        itm = reader.getCurrent();
        assertEquals(1462132862000000L, itm.getTimestampUsec());
        assertEquals("device-9", itm.getDeviceName());
        assertEquals(-39, itm.getSignalStrength());
        assertTrue(itm.hasMacAddress());

        assertFalse("no more records", reader.advance());
        reader.close();
    }

    @Test
    public void testSerializable() {
        SerializableUtils.serializeToByteArray(source);
    }
}
