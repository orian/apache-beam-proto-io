package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class ProtoIOSinkTest {
    private PipelineOptions opts;

    private static Path temp;

    @BeforeClass
    public static void setUpClass() throws IOException {
        temp = Files.createTempDirectory("proto-io-test-");
        System.out.println("created: "+temp);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        Files.delete(temp);
    }

    public static ProtoIOSink<Void,Data.RawItem> create(
            String prefix,
            Compression compression) {

        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(prefix);
        ValueProvider.StaticValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resource);
        DefaultFilenamePolicy policy = DefaultFilenamePolicy.fromStandardParameters(
                valueProvider, null, ".pb", false);

        ResourceId tmpDir = FileSystems.matchNewResource(temp.toString(), true);

        return new ProtoIOSink(ValueProvider.StaticValueProvider.of(tmpDir),
                DynamicFileDestinations.constant(policy), compression);
    }

    @Test
    public void baseTest() {
        ProtoIOSink<Void, Data.RawItem> sink = create(
                "somefile", Compression.UNCOMPRESSED);
        sink.validate(opts);

        ResourceId resourceId = (ResourceId) sink.getTempDirectoryProvider().get();
    }

    @Test
    public void testSimpleWrite() throws Exception {
        ProtoIOSink<Void, Data.RawItem> sink = create(
                "somefile", Compression.UNCOMPRESSED);
        sink.validate(opts);

        FileBasedSink.WriteOperation<Void,Data.RawItem> wo = sink.createWriteOperation();

        FileBasedSink.Writer<Void,Data.RawItem> writer = wo.createWriter();
        writer.openUnwindowed("unique1", 0, null);
        Data.RawItem item = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(item);

        Set<ResourceId> results = new HashSet<>();
        results.add(writer.close().getTempFilename());
//        wo.finalize(results);
        wo.removeTemporaryFiles(results);

        File f = new File(temp+"/somefile-00000-of-00001.pb");
        assertEquals(16, f.length());
        f.delete();
        // Copy input files to output files.
    }

    @Test
    public void testWriteGzip() throws Exception {
        final String dest = temp.toString() + "/myfile";
        ProtoIOSink<Void,Data.RawItem> sink = create("myfile", Compression.GZIP);
        sink.validate(opts);

        ProtoIOSink.WriteOperation<Void,Data.RawItem> wo = sink.createWriteOperation();

        FileBasedSink.Writer<Void,Data.RawItem> writer = wo.createWriter();
        writer.openUnwindowed("unique1", 0, null);
        Data.RawItem item = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(item);

        Set<ResourceId> results = new HashSet<>();
        results.add(writer.close().getTempFilename());
//        wo.finalize(results);
        wo.removeTemporaryFiles(results);

        File f = new File(dest+"-00000-of-00001.pb.gz");
        assertEquals(36, f.length());
        f.delete();
    }
}
