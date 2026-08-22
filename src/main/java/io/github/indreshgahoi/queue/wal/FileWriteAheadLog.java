package io.github.indreshgahoi.queue.wal;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class FileWriteAheadLog implements WriteAheadLog {

    private static final String SEPARATOR = "\\|";
    private static final String WRITE_SEPARATOR = "|";

    private final Path path;
    private final FileChannel channel;

    public FileWriteAheadLog(Path path) {
        this.path = Objects.requireNonNull(path, "path");
        try {
            this.channel = FileChannel.open(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to open WAL: " + path,
                    e
            );
        }
    }

    @Override
    public void append(WalRecord record) {
        Objects.requireNonNull(record, "record");
        String serialized = serialize(record) + System.lineSeparator();
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(serialized);
        try {
            /*
             * FileChannel.write() is not guaranteed to write the
             * entire buffer in one invocation.
             */
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            /*
             * v0.8 durability contract:
             *
             * A WAL append is considered successful only after
             * we request the file changes to be forced to storage.
             */
            channel.force(true);
        } catch (IOException e) {
            throw new WalException(
                    "Failed to append WAL record: " + path,
                    e
            );
        }

    }

    @Override
    public List<WalRecord> readAll() {
        List<WalRecord> records = new ArrayList<>();
        try (
                FileChannel readChannel = FileChannel.open(
                        path,
                        StandardOpenOption.READ
                );

                BufferedReader reader =
                        new BufferedReader(
                                Channels.newReader(
                                        readChannel,
                                        StandardCharsets.UTF_8
                                )
                        );
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                records.add(deserialize(line));
            }
            return List.copyOf(records);
        } catch (IOException e) {
            throw new WalException(
                    "Failed to read WAL: " + path,
                    e
            );
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new WalException("Failed to close WAL: " + path,
                    e);
        }
    }

    private String serialize(WalRecord record) {
        return record.type().name()
                + WRITE_SEPARATOR
                + encode(record.messageId())
                + WRITE_SEPARATOR
                + encode(record.payload())
                + WRITE_SEPARATOR
                + encode(record.receiptHandle())
                + WRITE_SEPARATOR
                + record.attempt()
                + WRITE_SEPARATOR
                + record.timestamp().toEpochMilli();
    }

    private WalRecord deserialize(String line) {
        String[] parts = line.split(SEPARATOR, -1);

        if (parts.length != 6) {
            throw new WalException(
                    "Invalid WAL record: expected 6 fields but found "
                            + parts.length
            );
        }

        try {
            WalRecordType type =
                    WalRecordType.valueOf(parts[0]);

            String messageId = decode(parts[1]);
            String payload = decode(parts[2]);
            String receiptHandle = decode(parts[3]);

            int attempt =
                    Integer.parseInt(parts[4]);

            Instant timestamp =
                    Instant.ofEpochMilli(
                            Long.parseLong(parts[5])
                    );

            return new WalRecord(
                    type,
                    messageId,
                    payload,
                    receiptHandle,
                    attempt,
                    timestamp
            );

        } catch (RuntimeException e) {
            throw new WalException(
                    "Unable to deserialize WAL record",
                    e
            );
        }
    }

    private String encode(String value) {
        if (value == null) {
            return "";
        }
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        value.getBytes(StandardCharsets.UTF_8)
                );
    }

    private String decode(String value) {
        if (value.isEmpty()) {
            return null;
        }
        return new String(
                Base64.getUrlDecoder().decode(value),
                StandardCharsets.UTF_8);
    }
}
