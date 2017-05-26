package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOTest {
    static String testFilePath = "src/test/java/eu/pawelsz/apache/beam/io/protoio/test.pb.bin";

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
