package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ProtoIOSinkTest {
    ProtoIOSink<Data.RawItem> sink;
    PipelineOptions opts;

    @Before
    public void setUp() {

//        sink = ProtoIO.write("/tmp/some-file", "pb.bin",
//                FileBasedSink.CompressionType.UNCOMPRESSED);
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible("somefile");
        ValueProvider.StaticValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resource);
        DefaultFilenamePolicy policy = DefaultFilenamePolicy.constructUsingStandardParameters(
                valueProvider, null, null);

        ResourceId tmpDir = FileSystems.matchNewResource("/tmp/proto-io-test", true);

//        "/tmp/";
        sink = new ProtoIOSink(ValueProvider.StaticValueProvider.of(tmpDir), policy);
    }

    @Test
    public void baseTest() {
        sink.validate(opts);

        ResourceId resourceId = sink.getBaseOutputDirectoryProvider().get();
    }

    @Test
    public void testSimpleWrite() throws Exception {
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
    }
}
