package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.WalPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private final SegmentPromoter segmentPromoter;
    private final FrameWriter frameWriter;
    private final ActiveSegmentOpener activeSegmentOpener;

    private long activeSegmentId;
    private FileChannel activeChannel;
    private boolean closed;
    private boolean poisoned = false;

    public SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes
    ) {
        this(
                walDirectory,
                segmentTargetBytes,
                SegmentedFileWriteAheadLog::promoteSegment,
                SegmentedFileWriteAheadLog::writeFrame,
                SegmentedFileWriteAheadLog::openAppendChannel
        );
    }

    /*
     * Package-private constructor for deterministic
     * segment-promotion failure tests.
     */
    SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes,
            SegmentPromoter segmentPromoter,
            FrameWriter frameWriter,
            ActiveSegmentOpener activeSegmentOpener
    ) {
        this.walDirectory =
                Objects.requireNonNull(
                        walDirectory,
                        "walDirectory"
                );

        this.segmentPromoter =
                Objects.requireNonNull(
                        segmentPromoter,
                        "segmentPromoter"
                );
        this.frameWriter =
                Objects.requireNonNull(
                        frameWriter,
                        "frameWriter"
                );
        this.activeSegmentOpener =
                Objects.requireNonNull(
                        activeSegmentOpener,
                        "activeSegmentOpener"
                );

        if (segmentTargetBytes
                <= WalSegmentInitializer.WAL_HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "segmentTargetBytes must be larger than WAL header size"
            );
        }

        this.segmentTargetBytes = segmentTargetBytes;
        this.segmentFiles = new WalSegmentFiles();
        this.discovery = new WalSegmentDiscovery();
        this.initializer = new WalSegmentInitializer();
        this.frameCodec = new WalFrameCodec();

        initialize();
    }

    @Override
    public synchronized void append(WalRecord record) {
        ensureWritable();

        Objects.requireNonNull(
                record,
                "record"
        );

        /*
         * Rotation has a different durability boundary from frame writing.
         * A failure before promotion leaves the current segment authoritative
         * and must remain retryable; rotate() owns that failure policy.
         */
        rotateIfNeeded();

        ByteBuffer frame =
                frameCodec.encode(record);

        try {
            frameWriter.write(
                    activeChannel,
                    frame
            );

            activeChannel.force(true);

        } catch (IOException e) {
            poisoned = true;
            throw new WalException(
                    "Failed to append WAL record",
                    e
            );
        }
    }

    @Override
    public synchronized List<WalRecord> readAll() {
        ensureOpen();

        List<WalSegment> segments =
                discovery.discover(
                        walDirectory
                );

        List<WalRecord> records =
                new ArrayList<>();

        for (int index = 0;
             index < segments.size();
             index++) {

            WalSegment segment = segments.get(index);
            SegmentRole role =
                    roleOf(index, segments.size());

            records.addAll(
                    readSegment(
                            segment,
                            role
                    )
            );
        }

        return List.copyOf(records);
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
    public synchronized List<WalRecord> readFrom(
            WalPosition position
    ) {
        ensureOpen();

        Objects.requireNonNull(
                position,
                "position"
        );

        List<WalSegment> segments =
                discovery.discover(
                        walDirectory
                );

        int startIndex =
                findSegmentIndex(
                        segments,
                        position.segmentId()
                );

        List<WalRecord> records =
                new ArrayList<>();

        for (int index = startIndex;
             index < segments.size();
             index++) {

            WalSegment segment = segments.get(index);
            SegmentRole role =
                    roleOf(index, segments.size());

            if (index == startIndex) {
                records.addAll(
                        readSegmentFrom(
                                segment,
                                role,
                                position.offset()
                        )
                );
            } else {
                records.addAll(
                        readSegment(
                                segment,
                                role
                        )
                );
            }
        }

        return List.copyOf(records);
    }

    @Override
    public synchronized boolean hasCompleteHistory() {
        ensureOpen();

        List<WalSegment> segments =
                discovery.discover(
                        walDirectory
                );

        return !segments.isEmpty()
                && segments.getFirst().segmentId() == 0;
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

            validateSegments(segments);
            openActiveSegment(segments.getLast());

        } catch (IOException e) {
            throw new WalException(
                    "Failed to initialize segmented WAL: "
                            + walDirectory,
                    e
            );
        }
    }

    private void validateSegments(
            List<WalSegment> segments
    ) {
        for (WalSegment segment : segments) {
            initializer.validate(segment.path());
        }

        for (int index = 0;
             index < segments.size();
             index++) {

            readSegment(
                    segments.get(index),
                    roleOf(index, segments.size())
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

        initializer.initialize(path);
        openActiveSegment(
                new WalSegment(segmentId, path)
        );
    }

    private void openActiveSegment(
            WalSegment segment
    ) throws IOException {
        activeSegmentId = segment.segmentId();
        activeChannel = activeSegmentOpener.open(segment.path());
    }

    private void rotateIfNeeded() {
        final long activeSegmentSize;

        try {
            activeSegmentSize = activeChannel.size();
        } catch (IOException e) {
            throw new WalException(
                    "Failed to inspect active WAL segment size",
                    e
            );
        }

        if (activeSegmentSize >= segmentTargetBytes) {
            rotate();
        }
    }

    private void rotate() {
        long nextSegmentId = activeSegmentId + 1;

        Path candidate =
                segmentFiles.tempPathFor(
                        walDirectory,
                        nextSegmentId
                );

        Path destination =
                segmentFiles.pathFor(
                        walDirectory,
                        nextSegmentId
                );

        if (Files.exists(destination)) {
            throw new WalException(
                    "Next WAL segment already exists: "
                            + destination
            );
        }

        boolean promoted = false;

        try {
            /*
             * A .tmp file is never authoritative. Under the WAL's
             * single-writer invariant, an abandoned candidate is safe to
             * discard before retrying this rotation.
             */
            Files.deleteIfExists(candidate);

            /*
             * initialize() writes and forces the candidate header before
             * the atomic authority transition below.
             */
            initializer.initialize(candidate);

            segmentPromoter.promote(
                    candidate,
                    destination
            );
            promoted = true;

            /*
             * The promoted .wal is now authoritative. Opening must use the
             * injected seam so post-promotion failures are observable and
             * handled as restart-required failures.
             */
            FileChannel newChannel =
                    activeSegmentOpener.open(destination);

            FileChannel previousChannel = activeChannel;

            activeChannel = newChannel;
            activeSegmentId = nextSegmentId;

            previousChannel.close();

        } catch (IOException e) {
            if (promoted) {
                /*
                 * Durable authority has moved, while this instance may still
                 * reference the previous segment. Continuing could append to
                 * the wrong segment, so a restart is required.
                 */
                poisoned = true;
            } else {
                deleteCandidateBestEffort(candidate);
            }

            throw new WalException(
                    "Failed to rotate WAL from segment "
                            + activeSegmentId
                            + " to "
                            + nextSegmentId,
                    e
            );
        }
    }

    private static void deleteCandidateBestEffort(
            Path candidate
    ) {
        try {
            Files.deleteIfExists(candidate);
        } catch (IOException ignored) {
            /*
             * The candidate is non-authoritative. A later rotation attempt
             * will retry the deletion before initialization.
             */
        }
    }

    private List<WalRecord> readSegment(
            WalSegment segment,
            SegmentRole role
    ) {
        try (FileChannel channel =
                     openForRead(segment, role)) {

            channel.position(
                    WalSegmentInitializer.WAL_HEADER_SIZE
            );

            return readFrames(
                    channel,
                    segment,
                    role
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read "
                            + role.description
                            + " WAL segment "
                            + segment.segmentId(),
                    e
            );
        }
    }

    private List<WalRecord> readSegmentFrom(
            WalSegment segment,
            SegmentRole role,
            long offset
    ) {
        try (FileChannel channel =
                     openForRead(segment, role)) {

            validateReplayPosition(
                    channel,
                    segment,
                    offset
            );

            channel.position(offset);

            return readFrames(
                    channel,
                    segment,
                    role
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to read WAL segment "
                            + segment.segmentId()
                            + " from offset "
                            + offset,
                    e
            );
        }
    }

    private List<WalRecord> readFrames(
            FileChannel channel,
            WalSegment segment,
            SegmentRole role
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
                    if (role == SegmentRole.SEALED) {
                        throw new WalException(
                                "Torn frame in sealed WAL segment "
                                        + segment.segmentId()
                                        + " at offset "
                                        + frame.frameStart()
                        );
                    }

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
            WalSegment segment,
            long requestedOffset
    ) throws IOException {
        long segmentEnd = channel.size();

        if (requestedOffset
                < WalSegmentInitializer.WAL_HEADER_SIZE) {
            throw new WalException(
                    "WAL offset is before segment record area: "
                            + requestedOffset
            );
        }

        if (requestedOffset > segmentEnd) {
            throw new WalException(
                    "WAL offset is beyond end of segment "
                            + segment.segmentId()
                            + ": "
                            + requestedOffset
                            + ", segment size: "
                            + segmentEnd
            );
        }

        if (requestedOffset
                == WalSegmentInitializer.WAL_HEADER_SIZE
                || requestedOffset == segmentEnd) {
            return;
        }

        channel.position(
                WalSegmentInitializer.WAL_HEADER_SIZE
        );

        while (channel.position() < segmentEnd) {
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

    private static int findSegmentIndex(
            List<WalSegment> segments,
            long segmentId
    ) {
        for (int index = 0;
             index < segments.size();
             index++) {

            if (segments.get(index).segmentId()
                    == segmentId) {
                return index;
            }
        }

        throw new WalException(
                "Unknown WAL segment: "
                        + segmentId
        );
    }

    private static SegmentRole roleOf(
            int index,
            int segmentCount
    ) {
        return index == segmentCount - 1
                ? SegmentRole.ACTIVE
                : SegmentRole.SEALED;
    }

    private static FileChannel openForAppend(
            Path path
    ) throws IOException {
        return FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    private static FileChannel openForRead(
            WalSegment segment,
            SegmentRole role
    ) throws IOException {
        if (role == SegmentRole.ACTIVE) {
            return FileChannel.open(
                    segment.path(),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
        }

        return FileChannel.open(
                segment.path(),
                StandardOpenOption.READ
        );
    }

    private static void truncateTail(
            FileChannel channel,
            long frameStart
    ) throws IOException {
        channel.truncate(frameStart);
        channel.force(true);
    }

    static void promoteSegment(
            Path candidate,
            Path destination
    ) throws IOException {
        Files.move(
                candidate,
                destination,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    static void writeFrame(
            FileChannel channel,
            ByteBuffer frame
    ) throws IOException {

        while (frame.hasRemaining()) {
            channel.write(frame);
        }
    }

    static FileChannel openAppendChannel(
            Path path
    ) throws IOException {

        return FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    private void ensureOpen() {
        if (closed) {
            throw new WalException(
                    "Segmented WAL is closed"
            );
        }
    }

    private void ensureWritable() {
        ensureOpen();

        if (poisoned) {
            throw new WalException(
                    "WAL is poisoned after previous append failure"
            );
        }
    }

    private enum SegmentRole {
        SEALED("sealed"),
        ACTIVE("active");

        private final String description;

        SegmentRole(String description) {
            this.description = description;
        }
    }

    @FunctionalInterface
    interface ActiveSegmentOpener {

        FileChannel open(
                Path path
        ) throws IOException;
    }

    @FunctionalInterface
    interface SegmentPromoter {

        void promote(
                Path candidate,
                Path destination
        ) throws IOException;
    }

    @FunctionalInterface
    interface FrameWriter {

        void write(
                FileChannel channel,
                ByteBuffer frame
        ) throws IOException;
    }
}
