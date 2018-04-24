package eu.pawelsz.apache.beam.io.protoio;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static eu.pawelsz.apache.beam.io.protoio.ProtoIOGzipTest.ALL;

public class ProtoIOTest {
    static String testFilePath = "src/test/java/eu/pawelsz/apache/beam/io/protoio/test.pb.bin";
    static String testFilesPath = "src/test/java/eu/pawelsz/apache/beam/io/protoio/test-*";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void duringPipeline() {
        PCollection<Data.RawItem> input = pipeline.apply(
                ProtoIO.read(Data.RawItem.class).from(testFilePath));
        input.apply(ProtoIO.write(Data.RawItem.class).to("/tmp/proto-io-test/out-1")
                .withoutSharding());

        PAssert.that(input).containsInAnyOrder(ALL);
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(NeedsRunner.class)
    public void manyFiles() {
        PCollection<Data.RawItem> input = pipeline.apply(
                ProtoIO.read(Data.RawItem.class).from(testFilesPath));
        input.apply(ProtoIO.write(Data.RawItem.class).to("/tmp/proto-io-test/out-2")
                .withoutSharding().withCompression(Compression.BZIP2));

//        PAssert.that(input).containsInAnyOrder(ALL);
        pipeline.run().waitUntilFinish();
    }
}
