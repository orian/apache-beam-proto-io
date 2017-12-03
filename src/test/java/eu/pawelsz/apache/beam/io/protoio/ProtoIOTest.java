package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ProtoIOTest {
    static String testFilePath = "src/test/java/eu/pawelsz/apache/beam/io/protoio/test-*.pb.bin";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void duringPipeline() {
        PCollection<Data.RawItem> input = pipeline.apply(
                Read.from(ProtoIO.source(Data.RawItem.class, testFilePath)));
        input.apply(ProtoIO.write("/tmp/proto-io-test/out"));
        pipeline.run().waitUntilFinish();
    }
}
