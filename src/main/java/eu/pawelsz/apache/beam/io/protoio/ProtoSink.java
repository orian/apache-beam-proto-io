package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.Message;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ProtoSink<UserT, DestinationT, OutputT extends Message>
        extends FileBasedSink<UserT, DestinationT, OutputT>
        implements Serializable {

    public ProtoSink(ValueProvider<ResourceId> tempDirectoryProvider,
                     DynamicDestinations<UserT, DestinationT, OutputT> dynamicDestinations) {
        super(tempDirectoryProvider, dynamicDestinations);
    }

    public ProtoSink(ValueProvider<ResourceId> tempDirectoryProvider,
                     DynamicDestinations<UserT, DestinationT, OutputT> dynamicDestinations,
                     WritableByteChannelFactory writableByteChannelFactory) {
        super(tempDirectoryProvider, dynamicDestinations, writableByteChannelFactory);
    }

    public ProtoSink(ValueProvider<ResourceId> tempDirectoryProvider,
                     DynamicDestinations<UserT, DestinationT, OutputT> dynamicDestinations,
                     Compression compression) {
        super(tempDirectoryProvider, dynamicDestinations, compression);
    }

    @Override
    public WriteOperation<DestinationT, OutputT> createWriteOperation() {
        return new ProtoWriteOperation<>(this);
    }

    private static class ProtoWriteOperation<DestinationT, OutputT extends Message>
            extends WriteOperation<DestinationT, OutputT> {
        private ProtoWriteOperation(ProtoSink<?, DestinationT, OutputT> sink) {
            super(sink);
        }

        @Override
        public Writer<DestinationT, OutputT> createWriter() throws Exception {
            return new ProtoWriter<>(this);
        }
    }

    public static class ProtoWriter<DestinationT, OutputT extends Message>
            extends Writer<DestinationT, OutputT> {
        OutputStream outputStream;

        public ProtoWriter(WriteOperation<DestinationT, OutputT> writeOperation) {
            super(writeOperation, MimeTypes.BINARY);
        }

        @Override
        protected void prepareWrite(WritableByteChannel writableByteChannel) throws Exception {
            this.outputStream = Channels.newOutputStream(writableByteChannel);
        }

        @Override
        public void write(OutputT t) throws Exception {
            t.writeDelimitedTo(outputStream);
        }

        @Override
        protected void finishWrite() throws Exception {
            outputStream.flush();
        }
    }
}
