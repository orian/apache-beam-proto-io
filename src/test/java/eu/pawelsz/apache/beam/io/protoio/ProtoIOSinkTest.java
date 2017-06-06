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
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ProtoIOSinkTest {
    PipelineOptions opts;

    static Path temp;

    @BeforeClass
    public static void setUpClass() throws IOException {
        temp = Files.createTempDirectory("proto-io-test-");
        System.out.println("created: "+temp);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        Files.delete(temp);
    }

    public static ProtoIOSink<Data.RawItem> create(
            String prefix,
            FileBasedSink.CompressionType compressionType) {

        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(prefix);
        ValueProvider.StaticValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resource);
        DefaultFilenamePolicy policy = DefaultFilenamePolicy.constructUsingStandardParameters(
                valueProvider, null, ".pb");

        ResourceId tmpDir = FileSystems.matchNewResource(temp.toString(), true);

        return new ProtoIOSink(ValueProvider.StaticValueProvider.of(tmpDir), policy, compressionType);
    }

    @Test
    public void baseTest() {
        ProtoIOSink<Data.RawItem> sink = create(
                "somefile", FileBasedSink.CompressionType.UNCOMPRESSED);
        sink.validate(opts);

        ResourceId resourceId = sink.getBaseOutputDirectoryProvider().get();
    }

    @Test
    public void testSimpleWrite() throws Exception {
        ProtoIOSink<Data.RawItem> sink = create(
                "somefile", FileBasedSink.CompressionType.UNCOMPRESSED);
        sink.validate(opts);

        FileBasedSink.WriteOperation<Data.RawItem> wo = sink.createWriteOperation();

        FileBasedSink.Writer<Data.RawItem> writer = wo.createWriter();
        writer.openUnwindowed("unique1", 0);
        Data.RawItem item = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(item);

        List<FileBasedSink.FileResult> results = new ArrayList<FileBasedSink.FileResult>();
        results.add(writer.close());
        wo.finalize(results);

        File f = new File(temp+"/somefile-00000-of-00001.pb");
        assertEquals(16, f.length());
        f.delete();
    }

    @Test
    public void testWriteGzip() throws Exception {
        final String dest = temp.toString() + "/myfile";
        ProtoIOSink<Data.RawItem> sink = create("myfile", FileBasedSink.CompressionType.GZIP);
        sink.validate(opts);

        ProtoIOSink.WriteOperation<Data.RawItem> wo = sink.createWriteOperation();

        FileBasedSink.Writer<Data.RawItem> writer = wo.createWriter();
        writer.openUnwindowed("unique1", 0);
        Data.RawItem item = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(item);

        List<FileBasedSink.FileResult> results = new ArrayList<FileBasedSink.FileResult>();
        results.add(writer.close());
        wo.finalize(results);

        File f = new File(dest+"-00000-of-00001.pb.gz");
        assertEquals(36, f.length());
        f.delete();
    }
}
