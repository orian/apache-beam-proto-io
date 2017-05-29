package eu.pawelsz.apache.beam.io.protoio;

import com.google.protobuf.Message;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ProtoIOSink<T extends Message> extends FileBasedSink<T> implements Serializable {

    public ProtoIOSink(ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy) {
        super(baseOutputDirectoryProvider, filenamePolicy);
    }

    public ProtoIOSink(ValueProvider<ResourceId> baseOutputDirectoryProvider, FilenamePolicy filenamePolicy,
                       WritableByteChannelFactory writableByteChannelFactory) {
        super(baseOutputDirectoryProvider, filenamePolicy, writableByteChannelFactory);
    }

    @Override
    public WriteOperation<T> createWriteOperation() {
        return new ProtoIOWriteOperation(this);
    }


    public static class ProtoIOWriteOperation<T extends Message> extends WriteOperation<T> {
        public ProtoIOWriteOperation(ProtoIOSink<T> sink) {
            super(sink);
        }

        @Override
        public Writer createWriter() throws Exception {
            return new ProtoIOWriter(this);
        }
    }

    public static class ProtoIOWriter<T extends Message> extends Writer<T> {
        OutputStream outputStream;

        public ProtoIOWriter(WriteOperation<T> writeOperation) {
            super(writeOperation, MimeTypes.BINARY);
        }

        @Override
        protected void prepareWrite(WritableByteChannel writableByteChannel) throws Exception {
            this.outputStream = Channels.newOutputStream(writableByteChannel);
        }

        @Override
        public void write(T t) throws Exception {
            t.writeDelimitedTo(outputStream);
        }

        @Override
        protected void finishWrite() throws Exception {
            super.finishWrite();
            outputStream.flush();
        }
    }
}
