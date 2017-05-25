package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import eu.pawelsz.apache.beam.io.protoio.Data;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ProtoIOSinkTest {
    ProtoIO.Sink<Data.RawItem> sink;
    PipelineOptions opts;

    @Before
    public void setUp() {

//        sink = ProtoIO.write("/tmp/some-file", "pb.bin",
//                FileBasedSink.CompressionType.UNCOMPRESSED);
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible("/tmp/somefile");
        DefaultFilenamePolicy policy = DefaultFilenamePolicy.constructUsingStandardParameters(
                ValueProvider.StaticValueProvider.of(resource), null, null);
        sink = new ProtoIO.Sink(ValueProvider.StaticValueProvider.of("/tmp/"), policy);
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
