package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOSourceTest {
    ProtoIOSource<Data.RawItem> source;
    PipelineOptions opts;

    static String testFilePath = "src/test/java/eu/pawelsz/apache/beam/io/protoio/test.pb.bin";

    @Before
    public void setUp() {
        source = ProtoIO.source(Data.RawItem.class, testFilePath);
    }

    @Test
    public void testEstimatedSizeBytes() throws Exception {
        assertEquals(390, source.getEstimatedSizeBytes(opts));
    }

    @Test
    public void testReadRecords() throws Exception {
        List<? extends FileBasedSource<Data.RawItem>> split = source.split(100000, opts);
        assertEquals(1, split.size());
        source = (ProtoIOSource<Data.RawItem>) split.get(0);
        ProtoIOSource.ProtoReader<Data.RawItem> reader = (ProtoIOSource.ProtoReader<Data.RawItem>) source.createSingleFileReader(opts);
        assertTrue("must read 0", reader.start()); // start reading
        Data.RawItem itm = reader.getCurrent();
        assertEquals(1462100462000000L, itm.getTimestampUsec());
        assertEquals("device-0", itm.getDeviceName());
        assertEquals(-30, itm.getSignalStrength());
        assertTrue(itm.hasMacAddress());

        for (int i=1;i<10;i++) {
            assertTrue("must read "+i, reader.advance());
        }
        assertFalse(reader.advance());
        reader.close();
    }

    @Test
    public void testSerializable () {
        SerializableUtils.serializeToByteArray(source);
    }
}
