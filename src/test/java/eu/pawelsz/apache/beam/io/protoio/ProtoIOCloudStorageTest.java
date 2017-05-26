package eu.pawelsz.apache.beam.io.protoio;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ProtoIOCloudStorageTest {
  BoundedSource<Data.RawItem> source;
  static PipelineOptions opts;

  static String testFilePath = "gs://datainq-dev/testdata/sharded_binary/test.pb.bin-00000-of-00002";
  static String testFilePathGz = "gs://datainq-dev/testdata/sharded_gz/test.pb-00000-of-00002.gz";

  @BeforeClass
  public static void setUpClass() {
    opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    FileSystems.setDefaultPipelineOptions(opts);
//    ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader());

//    IOChannelUtils.registerIOFactories(opts);
  }

  @Before
  public void setUp() {
//    opts = PipelineOptionsFactory.create();
//    opts.setRunner(DirectRunner.class);
    source = ProtoIOSource.from(Data.RawItem.class, testFilePath);
  }

  @Test
  public void testCloudStorageConnection() throws Exception {

  }

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    assertEquals(390, source.getEstimatedSizeBytes(opts));
  }

  @Test
  public void testReadRecords() throws IOException {
    BoundedSource.BoundedReader<Data.RawItem> reader = source.createReader(opts);
    assertTrue("must read 0", reader.start()); // start reading
    Data.RawItem itm = reader.getCurrent();
    assertEquals(1462100462000000L, itm.getTimestampUsec());
    assertEquals("device-0", itm.getDeviceName());
    assertEquals(-30, itm.getSignalStrength());
    assertTrue(itm.hasMacAddress());

    for (int i=1;i<10;i++) {
        assertTrue("must read "+i, reader.advance());
    }
    assertFalse(reader.advance());
    reader.close();
  }

  @Test
  public void testReadManyFiles() throws Exception {
    final int mul = 2;
    ProtoIOSource<Data.RawItem> localSource =
        ProtoIOSource.from(Data.RawItem.class, "gs://datainq-dev/testdata/sharded_binary/test.pb.bin-*");

    assertEquals(390*mul, localSource.getEstimatedSizeBytes(opts));

    int filesInBucket = 1;
    List<? extends BoundedSource<Data.RawItem>> bundles = localSource.split(filesInBucket * 390, opts);
    assertEquals(2, bundles.size());

    for (BoundedSource<Data.RawItem> src : bundles) {
      assertEquals(390*filesInBucket, src.getEstimatedSizeBytes(opts));
      BoundedSource.BoundedReader<Data.RawItem> reader = src.createReader(opts);
      assertTrue("must read 0", reader.start()); // start reading
      for (int i=1;i<10*filesInBucket; i++) {
        assertTrue("must read "+i, reader.advance());
      }
      assertFalse(reader.advance());
      reader.close();
    }
    {
      BoundedSource.BoundedReader<Data.RawItem> reader =
          localSource.createReader(opts);
      assertTrue("must read 0", reader.start()); // start reading
      for (int i=1;i<10*mul;i++) {
        assertTrue("must read "+i, reader.advance());
      }
      assertFalse(reader.advance());
      reader.close();
    }
  }

  @Test
  public void testSerializable () {
    SerializableUtils.serializeToByteArray(source);
  }

  @Test
  public void testReadRecordsGzip() throws Exception {
    source = CompressedSource.from(ProtoIOSource.from(Data.RawItem.class, testFilePathGz));

    BoundedSource.BoundedReader<Data.RawItem> reader = source.createReader(opts);
    assertTrue("must read 0", reader.start()); // start reading
    Data.RawItem itm = reader.getCurrent();
    assertEquals(1462100462000000L, itm.getTimestampUsec());
    assertEquals("device-0", itm.getDeviceName());
    assertEquals(-30, itm.getSignalStrength());
    assertTrue(itm.hasMacAddress());

    for (int i=1;i<10;i++) {
      assertTrue("must read "+i, reader.advance());
    }
    assertFalse(reader.advance());
    reader.close();
  }

  @Test
  public void testReadCompressed() throws Exception {
    source = CompressedSource.from(
        ProtoIOSource.from(
            Data.RawItem.class,
            "gs://datainq-dev/testdata/single_gz/test.pb.gz-00001-of-00002"))
        .withDecompression(CompressedSource.CompressionMode.GZIP);


    assertEquals(199, source.getEstimatedSizeBytes(opts));

    BoundedSource.BoundedReader<Data.RawItem> reader = source.createReader(opts);
    assertTrue("must read 0", reader.start()); // start reading
    Data.RawItem itm = reader.getCurrent();
    assertEquals(1462100462000000L, itm.getTimestampUsec());
    assertEquals("device-0", itm.getDeviceName());
    assertEquals(-30, itm.getSignalStrength());
    assertTrue(itm.hasMacAddress());

    for (int i=1;i<10;i++) {
      assertTrue("must read "+i, reader.advance());
    }
    assertFalse(reader.advance());
    reader.close();
  }
}
