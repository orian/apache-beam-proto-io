package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOMultiTest {
    ProtoIOSource<Data.RawItem> source;
    PipelineOptions opts;

    @Before
    public void setUp() {
        source = ProtoIO.source(Data.RawItem.class, "/media/orian/RaidVol1/orian/workspace/HA_dataflow/src/test/java/pl/helloagain/test*.pb.bin");
    }

    @Test
    public void testEstimatedSizeBytes() throws Exception {
        assertEquals(790, source.getEstimatedSizeBytes(opts));
    }

//    @Test
//    public void testReadRecords() throws IOException {
//        ProtoIOSource.ProtoReader<Data.RawItem> reader =
//                (ProtoIOSource.ProtoReader<Data.RawItem>) source.createReader(opts);
//        assertTrue("must read 0", reader.start()); // start reading
//        Data.RawItem itm = reader.getCurrent();
//        assertEquals(1462100462000000L, itm.getTimestampUsec());
//        assertEquals("device-0", itm.getDeviceName());
//        assertEquals(-30, itm.getSignalStrength());
//        assertTrue(itm.hasMacAddress());
//
//        for (int i=1;i<20;i++) {
//            assertTrue("must read "+i, reader.advance());
//        }
//        assertFalse(reader.advance());
//        reader.close();
//    }

    @Test
    public void testReadManyFiles() throws Exception {
        final int mul = 2;
        ProtoIOSource<Data.RawItem> localSource = ProtoIO.source(Data.RawItem.class, "src/test/java/eu/pawelsz/apache/beam/io/protoio/test-*.pb.bin");

        assertEquals(390*mul, localSource.getEstimatedSizeBytes(opts));

        List<? extends BoundedSource<Data.RawItem>> bundles = localSource.split(390, opts);
        assertEquals(2, bundles.size());

        for (BoundedSource<Data.RawItem> src : bundles) {
            assertEquals(390, src.getEstimatedSizeBytes(opts));
            ProtoIOSource.ProtoReader<Data.RawItem> reader =
                    (ProtoIOSource.ProtoReader<Data.RawItem>) src.createReader(opts);
            assertTrue("must read 0", reader.start()); // start reading
            for (int i=1;i<10; i++) {
                assertTrue("must read "+i, reader.advance());
            }
            assertFalse(reader.advance());
            reader.close();
        }
//        {
//            ProtoIO.ProtoReader<Data.RawItem> reader =
//                    (ProtoIO.ProtoReader<Data.RawItem>) localSource.createReader(opts);
//            assertTrue("must read 0", reader.start()); // start reading
//            for (int i=1;i<10*mul;i++) {
//                assertTrue("must read "+i, reader.advance());
//            }
//            assertFalse(reader.advance());
//            reader.close();
//        }
    }

    @Test
    public void testSplit() throws Exception {
        List<? extends BoundedSource<Data.RawItem>> sources = source.split(2, opts);
        assertEquals(2, sources.size());
    }
}
