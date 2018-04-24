package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProtoIOSinkTest {
    private PipelineOptions opts;

    @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public transient TestPipeline p = TestPipeline.create();

    public ProtoSink<Data.RawItem,Void, Data.RawItem> create(
            String prefix,
            Compression compression) {

        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(prefix);
        ValueProvider.StaticValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resource);
        DefaultFilenamePolicy policy = DefaultFilenamePolicy.fromStandardParameters(
                valueProvider, null, ".pb", false);

        ResourceId tmpDir = FileSystems.matchNewResource(tempFolder.getRoot().toString(), true);

        return new ProtoSink(ValueProvider.StaticValueProvider.of(tmpDir),
                DynamicFileDestinations.constant(policy), compression);
    }

    @Test
    public void baseTest() {
        ProtoSink<Data.RawItem, Void, Data.RawItem> sink = create(
                "somefile", Compression.UNCOMPRESSED);
        sink.validate(opts);

        ResourceId resourceId = (ResourceId) sink.getTempDirectoryProvider().get();
    }

    @Test
    public void testSimpleWrite() throws Exception {
        ProtoSink<Data.RawItem, Void, Data.RawItem> sink = create(
                "somefile", Compression.UNCOMPRESSED);
        sink.validate(opts);

        FileBasedSink.WriteOperation<Void,Data.RawItem> wo = sink.createWriteOperation();

        FileBasedSink.Writer<Void,Data.RawItem> writer = wo.createWriter();
        wo.setWindowedWrites(false);
        writer.open("unique1");
        Data.RawItem RAW0 = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(RAW0);

        writer.close();

        File f = new File(writer.getOutputFile().toString());
        assertTrue(f.exists());
        assertEquals(16, f.length());
        writer.cleanup();
    }

    @Test
    public void testWriteGzip() throws Exception {
        final String dest = tempFolder.getRoot().toString() + "/myfile";
        ProtoSink<Data.RawItem, Void,Data.RawItem> sink = create("myfile", Compression.GZIP);
        sink.validate(opts);

        ProtoSink.WriteOperation<Void,Data.RawItem> wo = sink.createWriteOperation();
        wo.setWindowedWrites(false);

        FileBasedSink.Writer<Void,Data.RawItem> writer = wo.createWriter();
        writer.open("unique2");
        Data.RawItem RAW0 = Data.RawItem.newBuilder()
                .setTimestampUsec(123123123L).setDeviceName("device").setSignalStrength(80).build();
        writer.write(RAW0);
        writer.close();

        File f = new File(writer.getOutputFile().toString());
        assertTrue(f.exists());
        assertEquals(36, f.length());
        writer.cleanup();
    }

//    private void runTestWrite(Data.RawItem[] elems, String header, String footer, int numShards)
//            throws Exception {
//        String outputName = "file.txt";
//        Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "testwrite");
//        ResourceId baseFilename =
//                FileBasedSink.convertToFileResourceIfPossible(baseDir.resolve(outputName).toString());
//
//        PCollection<Data.RawItem> input =
//                p.apply("CreateInput", Create.of(Arrays.asList(elems)).withCoder(ProtoCoder.of(Data.RawItem.class)));
//
//        TextIO.TypedWrite<String, Void> write =
//                TextIO.write().to(baseFilename).withHeader(header).withFooter(footer).withOutputFilenames();
//
//        if (numShards == 1) {
//            write = write.withoutSharding();
//        } else if (numShards > 0) {
//            write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
//        }
//
//        input.apply(write);
//
//        p.run();
//
////        assertOutputFiles(
////                elems,
////                header,
////                footer,
////                numShards,
////                baseFilename,
////                firstNonNull(
////                        write.getShardTemplate(),
////                        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE));
//    }
}
