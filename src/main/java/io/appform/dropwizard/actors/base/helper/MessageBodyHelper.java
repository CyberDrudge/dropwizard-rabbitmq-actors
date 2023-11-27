package io.appform.dropwizard.actors.base.helper;

import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.compression.CompressionAlgorithm;
import io.appform.dropwizard.actors.compression.CompressionProvider;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;

import static io.appform.dropwizard.actors.utils.MessageHeaders.COMPRESSION_TYPE;

@Slf4j
public class MessageBodyHelper {

    private CompressionProvider compressionProvider = new CompressionProvider();
    public MessageBodyHelper() {
    }

    public byte[] decompressMessage(final byte[] message,
                                    final AMQP.BasicProperties properties) throws IOException {

        if (properties.getHeaders() != null && properties.getHeaders().containsKey(COMPRESSION_TYPE)) {
            log.info("Compressed message size: {}", message.length);
            val body = compressionProvider.decompress(message, (CompressionAlgorithm) properties.getHeaders()
                    .get(COMPRESSION_TYPE));
            log.info("Uncompressed message size: {}", body.length);
            return body;
        }
        return message;
    }

    public byte[] compressMessage(final byte[] message,
                                  final AMQP.BasicProperties properties) throws IOException {

        if (properties.getHeaders() != null && properties.getHeaders().containsKey(COMPRESSION_TYPE)) {
            log.info("Uncompressed message size: {}", message.length);
            val body = compressionProvider.compress(message, (CompressionAlgorithm) properties.getHeaders()
                    .get(COMPRESSION_TYPE));
            log.info("Compressed message size: {}", body.length);
            return body;
        }
        return message;
    }
}
