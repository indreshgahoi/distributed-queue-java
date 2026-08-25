package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SegmentedFileWriteAheadLog
        implements WriteAheadLog {

    private final Path walDirectory;
    private final long segmentTargetBytes;

    private final WalSegmentFiles segmentFiles;
    private final WalSegmentDiscovery discovery;
    private final WalSegmentInitializer initializer;
    private final WalFrameCodec frameCodec;

    private long activeSegmentId;
    private FileChannel activeChannel;

    private boolean closed;

    public SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes
    ) {
        this.walDirectory =
                Objects.requireNonNull(
                        walDirectory,
                        "walDirectory"
                );

        if (segmentTargetBytes
                <= WalSegmentInitializer.WAL_HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "segmentTargetBytes must be larger than WAL header size"
            );
        }

        this.segmentTargetBytes =
                segmentTargetBytes;

        this.segmentFiles =
                new WalSegmentFiles();

        this.discovery =
                new WalSegmentDiscovery();

        this.initializer =
                new WalSegmentInitializer();
        this.frameCodec = new WalFrameCodec();

        initialize();
    }

    private void initialize() {
        try {
            Files.createDirectories(
                    walDirectory
            );

            List<WalSegment> segments =
                    discovery.discover(
                            walDirectory
                    );

            if (segments.isEmpty()) {
                createInitialSegment();
                return;
            }

            /*
             * Every authoritative segment must have
             * a valid WAL header.
             */
            for (WalSegment segment : segments) {
                initializer.validate(
                        segment.path()
                );
            }

            /*
             * Highest authoritative .wal segment
             * is active by definition.
             */
            WalSegment active =
                    segments.getLast();

            this.activeSegmentId =
                    active.segmentId();

            this.activeChannel =
                    openForAppend(
                            active.path()
                    );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to initialize segmented WAL: "
                            + walDirectory,
                    e
            );
        }
    }

    private void createInitialSegment()
            throws IOException {

        long segmentId = 0;

        Path path =
                segmentFiles.pathFor(
                        walDirectory,
                        segmentId
                );

        initializer.initialize(
                path
        );

        this.activeSegmentId =
                segmentId;

        this.activeChannel =
                openForAppend(
                        path
                );
    }

    private FileChannel openForAppend(
            Path path
    ) throws IOException {

        return FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    @Override
    public synchronized WalPosition currentDurablePosition() {
        ensureOpen();

        try {
            return new WalPosition(
                    activeSegmentId,
                    activeChannel.size()
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read active WAL position",
                    e
            );
        }
    }

    @Override
    public synchronized void append(
            WalRecord record
    ) {
        ensureOpen();

        Objects.requireNonNull(
                record,
                "record"
        );

        ByteBuffer frame =
                frameCodec.encode(
                        record
                );

        try {
            while (frame.hasRemaining()) {
                activeChannel.write(frame);
            }

            activeChannel.force(true);

        } catch (IOException e) {
            throw new WalException(
                    "Failed to append WAL record",
                    e
            );
        }
    }

    @Override
    public synchronized List<WalRecord> readAll() {
        List<WalSegment> segments =
                discovery.discover(
                        walDirectory
                );

        if (segments.size() != 1) {
            throw new WalException(
                    "Cross-segment recovery not implemented yet"
            );
        }

        return readActiveSegment(
                segments.getFirst()
        );
    }

    private List<WalRecord> readActiveSegment(
            WalSegment segment
    ) {
        try (
                FileChannel channel =
                        FileChannel.open(
                                segment.path(),
                                StandardOpenOption.READ,
                                StandardOpenOption.WRITE
                        )
        ) {
            /*
             * Segment header was already validated during startup.
             * Records begin immediately after the fixed WAL header.
             */
            channel.position(
                    WalSegmentInitializer.WAL_HEADER_SIZE
            );

            return readRecordsFromActiveSegment(
                    channel
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read active WAL segment: "
                            + segment.path(),
                    e
            );
        }
    }

    private List<WalRecord> readRecordsFromActiveSegment(
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

    private static void truncateTail(
            FileChannel channel,
            long frameStart
    ) throws IOException {
        channel.truncate(frameStart);
        channel.force(true);
    }

    @Override
    public synchronized List<WalRecord> readFrom(
            WalPosition position
    ) {
        throw new UnsupportedOperationException(
                "readFrom not implemented yet"
        );
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        try {
            activeChannel.close();
            closed = true;

        } catch (IOException e) {
            throw new WalException(
                    "Failed to close segmented WAL",
                    e
            );
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new WalException(
                    "Segmented WAL is closed"
            );
        }
    }
}
