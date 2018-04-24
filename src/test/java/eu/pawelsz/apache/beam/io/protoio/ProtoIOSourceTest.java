package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static eu.pawelsz.apache.beam.io.protoio.ProtoIOGzipTest.ALL;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOSourceTest {
    ProtoSource<Data.RawItem> source;
    PipelineOptions opts;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();


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
        source = (ProtoSource<Data.RawItem>) split.get(0);
        ProtoSource.ProtoReader<Data.RawItem> reader =
                (ProtoSource.ProtoReader<Data.RawItem>) source.createSingleFileReader(opts);
        assertTrue("must read 0", reader.start()); // start reading
        for (int i = 0; i < 9; i++) {
            assertEquals(ALL.get(i), reader.getCurrent());
            assertTrue("must read " + i, reader.advance());
        }

        assertEquals(ALL.get(9), reader.getCurrent());
        assertFalse(reader.advance());
        reader.close();
    }

    @Test
    public void testSerializable () {
        SerializableUtils.serializeToByteArray(source);
    }
}
