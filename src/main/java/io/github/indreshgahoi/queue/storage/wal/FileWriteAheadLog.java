package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class FileWriteAheadLog
        implements WriteAheadLog {

    private static final int WAL_HEADER_SIZE =
            WalHeaderCodec.HEADER_SIZE;

    private final Path path;
    private final WalFrameCodec frameCodec;
    private final FileChannel appendChannel;
    private final FrameAppender frameAppender;
    private final WalHeaderCodec headerCodec;
    private final StorageLineage storageLineage;

    /*
     * If append fails, the current frame may have been
     * partially persisted. Reject subsequent appends until
     * a new instance performs recovery.
     */
    private boolean failed;
    private boolean closed;

    public FileWriteAheadLog(Path path) {
        this(
                path,
                null,
                FileWriteAheadLog::writeAndForceFrame
        );
    }

    public FileWriteAheadLog(
            Path path,
            StorageLineage storageLineage
    ) {
        this(
                path,
                Objects.requireNonNull(
                        storageLineage,
                        "storageLineage"
                ),
                FileWriteAheadLog::writeAndForceFrame
        );
    }

    /*
     * Package-private constructor for deterministic
     * failure-injection tests.
     */
    FileWriteAheadLog(
            Path path,
            FrameAppender frameAppender
    ) {
        this(
                path,
                null,
                frameAppender
        );
    }

    private FileWriteAheadLog(
            Path path,
            StorageLineage configuredLineage,
            FrameAppender frameAppender
    ) {
        this.path =
                Objects.requireNonNull(
                        path,
                        "path"
                );

        this.frameAppender =
                Objects.requireNonNull(
                        frameAppender,
                        "frameAppender"
                );

        this.frameCodec = new WalFrameCodec();
        this.headerCodec = new WalHeaderCodec();

        try {
            this.appendChannel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.APPEND
                    );

            this.storageLineage =
                    initializeOrValidateHeader(
                            configuredLineage
                    );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to open WAL: " + path,
                    e
            );
        }
    }

    @Override
    public synchronized void append(
            WalRecord record
    ) {
        ensureAppendable();

        ByteBuffer frame =
                frameCodec.encode(record);

        try {
            frameAppender.append(
                    appendChannel,
                    frame
            );

        } catch (IOException e) {
            failed = true;

            throw new WalException(
                    "WAL append failed; WAL requires recovery before further writes",
                    e
            );
        }
    }

    @Override
    public synchronized List<WalRecord> readAll() {
        ensureOpen();

        try (FileChannel recoveryChannel =
                     openRecoveryChannel()) {

            recoveryChannel.position(
                    WAL_HEADER_SIZE
            );

            return readRecordsFrom(
                    recoveryChannel
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to recover WAL: " + path,
                    e
            );
        }
    }

    @Override
    public synchronized WalPosition currentDurablePosition() {
        ensureOpen();

        try {
            return new WalPosition(
                    0,
                    appendChannel.size()
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read current WAL position",
                    e
            );
        }
    }

    @Override
    public synchronized List<WalRecord> readFrom(
            WalPosition position
    ) {
        ensureOpen();

        Objects.requireNonNull(
                position,
                "position"
        );

        if (position.segmentId() != 0) {
            throw new WalException(
                    "Unsupported WAL segment: "
                            + position.segmentId()
            );
        }

        try (FileChannel recoveryChannel =
                     openRecoveryChannel()) {

            validateReplayPosition(
                    recoveryChannel,
                    position.offset(),
                    recoveryChannel.size()
            );

            recoveryChannel.position(
                    position.offset()
            );

            return readRecordsFrom(
                    recoveryChannel
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read WAL from position: "
                            + position,
                    e
            );
        }
    }

    @Override
    public StorageLineage storageLineage() {
        return storageLineage;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        try {
            appendChannel.close();

        } catch (IOException e) {
            throw new WalException(
                    "Failed to close WAL: " + path,
                    e
            );
        }
    }

    private StorageLineage initializeOrValidateHeader(
            StorageLineage configuredLineage
    ) {
        try {
            if (appendChannel.size() == 0) {
                StorageLineage created =
                        configuredLineage == null
                                ? StorageLineage.create()
                                : configuredLineage;

                writeHeader(created);
                return created;
            }

            StorageLineage persisted =
                    readHeader();

            if (configuredLineage != null
                    && !configuredLineage.equals(persisted)) {
                throw new WalException(
                        "Configured storage lineage does not match WAL lineage"
                );
            }

            return persisted;

        } catch (IOException e) {
            throw new WalException(
                    "Failed to initialize WAL header: " + path,
                    e
            );
        }
    }

    private void writeHeader(
            StorageLineage lineage
    ) throws IOException {
        ByteBuffer header =
                headerCodec.encode(lineage);

        writeFully(
                appendChannel,
                header
        );

        appendChannel.force(true);
    }

    private StorageLineage readHeader() throws IOException {
        try (FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.READ
                     )) {

            return headerCodec.read(
                    channel,
                    "WAL"
            );
        }
    }

    private FileChannel openRecoveryChannel()
            throws IOException {
        return FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
    }

    private List<WalRecord> readRecordsFrom(
            FileChannel channel
    ) throws IOException {
        List<WalRecord> records =
                new ArrayList<>();

        while (true) {
            DecodedFrame frame =
                    frameCodec.readNext(channel);

            switch (frame.status()) {
                case COMPLETE ->
                        records.add(frame.record());

                case CLEAN_EOF -> {
                    return List.copyOf(records);
                }

                case TORN_LENGTH,
                     TORN_PAYLOAD,
                     TORN_CHECKSUM -> {
                    truncateTail(
                            channel,
                            frame.frameStart()
                    );

                    return List.copyOf(records);
                }
            }
        }
    }

    private void validateReplayPosition(
            FileChannel channel,
            long requestedOffset,
            long walEnd
    ) throws IOException {
        if (requestedOffset < WAL_HEADER_SIZE) {
            throw new WalException(
                    "WAL offset is before record area: "
                            + requestedOffset
            );
        }

        if (requestedOffset > walEnd) {
            throw new WalException(
                    "WAL offset is beyond end of file: "
                            + requestedOffset
                            + ", WAL size: "
                            + walEnd
            );
        }

        if (requestedOffset == WAL_HEADER_SIZE
                || requestedOffset == walEnd) {
            return;
        }

        channel.position(WAL_HEADER_SIZE);

        while (channel.position() < walEnd) {
            long frameStart = channel.position();

            if (frameStart == requestedOffset) {
                return;
            }

            DecodedFrame frame =
                    frameCodec.readNext(channel);

            if (frame.status()
                    != FrameReadStatus.COMPLETE) {
                break;
            }

            long frameEnd = channel.position();

            if (requestedOffset > frameStart
                    && requestedOffset < frameEnd) {
                throw new WalException(
                        "WAL offset does not point to a frame boundary: "
                                + requestedOffset
                );
            }

            if (frameEnd == requestedOffset) {
                return;
            }
        }

        throw new WalException(
                "WAL offset does not point to a valid frame boundary: "
                        + requestedOffset
        );
    }

    private static void writeAndForceFrame(
            FileChannel channel,
            ByteBuffer frame
    ) throws IOException {
        writeFully(
                channel,
                frame
        );

        channel.force(true);
    }

    private static void writeFully(
            FileChannel channel,
            ByteBuffer buffer
    ) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static boolean readFully(
            FileChannel channel,
            ByteBuffer buffer
    ) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                break;
            }
        }

        return !buffer.hasRemaining();
    }

    private static void truncateTail(
            FileChannel channel,
            long lastValidPosition
    ) throws IOException {
        channel.truncate(
                lastValidPosition
        );

        channel.force(true);
    }

    private void ensureOpen() {
        if (closed) {
            throw new WalException(
                    "WAL is already closed"
            );
        }
    }

    private void ensureAppendable() {
        ensureOpen();

        if (failed) {
            throw new WalException(
                    "WAL is in failed state; close and reopen it to recover"
            );
        }
    }

    /*
     * Failure-injection boundary. Production writes and forces the
     * complete frame; tests can write a partial frame and throw.
     */
    @FunctionalInterface
    interface FrameAppender {

        void append(
                FileChannel channel,
                ByteBuffer frame
        ) throws IOException;
    }
}
