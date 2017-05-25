package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;
import eu.pawelsz.apache.beam.io.protoio.Data;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOTest {
    ProtoIO.Source<Data.RawItem> source;
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
        source = (ProtoIO.Source<Data.RawItem>) split.get(0);
        ProtoIO.ProtoReader<Data.RawItem> reader = (ProtoIO.ProtoReader<Data.RawItem>) source.createSingleFileReader(opts);
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

//    @Test
//    public void testReadManyFiles() throws Exception {
//        LinkedList<String> paths = new LinkedList<>();
//        final int mul = 500;
//        for (int i = 0;i<mul; i++) {
//            paths.add(testFilePath);
//        }
//        ProtoIO.Source<Data.RawItem> localSource = ProtoIO.source(Data.RawItem.class, paths);
//
//        assertEquals(390*mul, localSource.getEstimatedSizeBytes(opts));
//
//        int filesInBucket = 10;
//        List<? extends BoundedSource<Data.RawItem>> bundles = localSource.splitIntoBundles(filesInBucket * 390, opts);
//        assertEquals(50, bundles.size());
//
//        for (BoundedSource<Data.RawItem> src : bundles) {
//            assertEquals(390*filesInBucket, src.getEstimatedSizeBytes(opts));
//            ProtoIO.DeprecatedReader<Data.RawItem> reader =
//                    (ProtoIO.DeprecatedReader<Data.RawItem>) src.createReader(opts);
//            assertTrue("must read 0", reader.start()); // start reading
//            for (int i=1;i<10*filesInBucket; i++) {
//                assertTrue("must read "+i, reader.advance());
//            }
//            assertFalse(reader.advance());
//            reader.close();
//        }
//        {
//            ProtoIO.DeprecatedReader<Data.RawItem> reader =
//                    (ProtoIO.DeprecatedReader<Data.RawItem>) localSource.createReader(opts);
//            assertTrue("must read 0", reader.start()); // start reading
//            for (int i=1;i<10*mul;i++) {
//                assertTrue("must read "+i, reader.advance());
//            }
//            assertFalse(reader.advance());
//            reader.close();
//        }
//    }

    @Test
    public void testSerializable () {
        SerializableUtils.serializeToByteArray(source);
    }
}
