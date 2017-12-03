package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.Message;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoIO {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoIO.class);

    public static <T extends Message> ProtoIOSource<T> source (Class<T> recordClass, String fileOrPatternSpec) {
        return ProtoIOSource.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> ProtoIOSource<T> source (Class<T> recordClass, ValueProvider<String> fileOrPatternSpec) {
        return ProtoIOSource.from(recordClass, fileOrPatternSpec);
    }

    public static <T extends Message> ProtoIOSink<Void,T> sink(String baseOutputFilename) {
        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        // In this case, DestinationT == Void
        FileBasedSink.FilenamePolicy usedFilenamePolicy =
                    DefaultFilenamePolicy.fromStandardParameters(
                            valueProvider,
                            null,null,
                            false);

        usedFilenamePolicy = DefaultFilenamePolicy.fromParams(new DefaultFilenamePolicy.Params().withBaseFilename(resourceId));
        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);
//        dynamicDestinations =
//                (FileBasedSink.DynamicDestinations)
//                        DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());

        return new ProtoIOSink<Void, T>(valueProvider, dynamicDestinations);
    }

    public static <T extends Message> ProtoIOSink<Void, T> sink(String baseOutputFilename, String extension,
                                                          FileBasedSink.WritableByteChannelFactory factory) {

        ResourceId resourceId = FileBasedSink.convertToFileResourceIfPossible(baseOutputFilename);
        ValueProvider<ResourceId> valueProvider = ValueProvider.StaticValueProvider.of(resourceId);

        DefaultFilenamePolicy usedFilenamePolicy = DefaultFilenamePolicy.fromParams(
                new DefaultFilenamePolicy.Params().withBaseFilename(resourceId).withSuffix(extension));

        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);

        return new ProtoIOSink<Void, T>(valueProvider, dynamicDestinations, factory);
    }

    public static <T extends Message> ProtoIOSink<Void, T> sink(ValueProvider<String> baseOutputFilename,
                                                          String extension,
                                                                FileBasedSink.WritableByteChannelFactory factory) {
        ValueProvider<ResourceId> valueProvider = ValueProvider.NestedValueProvider.of(
                baseOutputFilename, FileBasedSink::convertToFileResourceIfPossible);

        DefaultFilenamePolicy usedFilenamePolicy = DefaultFilenamePolicy.fromParams(
                new DefaultFilenamePolicy.Params().withBaseFilename(valueProvider).withSuffix(extension));

        FileBasedSink.DynamicDestinations dynamicDestinations = DynamicFileDestinations.constant(usedFilenamePolicy);

        return new ProtoIOSink<Void, T>(valueProvider, dynamicDestinations, factory);
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            String baseOutputFilename) {
        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename));
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            String baseOutputFilename, String extension,
            FileBasedSink.WritableByteChannelFactory factory) {
        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename, extension, factory));
    }

    public static <T extends Message> PTransform<PCollection<T>, PDone> write(
            ValueProvider<String> baseOutputFilename, String extension,
            FileBasedSink.WritableByteChannelFactory factory) {
        return WriteFiles.<T,Void,T>to(sink(baseOutputFilename, extension, factory));
    }
}