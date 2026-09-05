package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.DirectoryDurability;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.AppendBatchResult;
import io.github.indreshgahoi.queue.storage.replication.HistoryReclaimedException;
import io.github.indreshgahoi.queue.storage.replication.LogConflictException;
import io.github.indreshgahoi.queue.storage.replication.LogEntry;
import io.github.indreshgahoi.queue.storage.replication.LogGapException;
import io.github.indreshgahoi.queue.storage.replication.LogPoint;
import io.github.indreshgahoi.queue.storage.replication.ReplicatedLog;

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
import java.util.Optional;

public final class SegmentedFileWriteAheadLog
        implements WriteAheadLog, ReplicatedLog {

    private final Path walDirectory;
    private final long segmentTargetBytes;
    private final WalSegmentFiles segmentFiles;
    private final WalSegmentDiscovery discovery;
    private final WalSegmentInitializer initializer;
    private final WalFrameCodec frameCodec;
    private final SegmentPromoter segmentPromoter;
    private final FrameWriter frameWriter;
    private final ActiveSegmentOpener activeSegmentOpener;
    private final DirectoryForcer directoryForcer;
    private final StorageLineage storageLineage;
    private final List<LogEntry> logicalEntries;
    private ChannelForcer channelForcer = FileChannel::force;

    private long activeSegmentId;
    private LogPoint snapshotBoundary = LogPoint.EMPTY;
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
                SegmentedFileWriteAheadLog::openAppendChannel,
                DirectoryDurability::forceParent,
                null
        );
    }

    public SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes,
            StorageLineage storageLineage
    ) {
        this(
                walDirectory,
                segmentTargetBytes,
                SegmentedFileWriteAheadLog::promoteSegment,
                SegmentedFileWriteAheadLog::writeFrame,
                SegmentedFileWriteAheadLog::openAppendChannel,
                DirectoryDurability::forceParent,
                Objects.requireNonNull(
                        storageLineage,
                        "storageLineage"
                )
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
        this(
                walDirectory,
                segmentTargetBytes,
                segmentPromoter,
                frameWriter,
                activeSegmentOpener,
                DirectoryDurability::forceParent,
                null
        );
    }

    SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes,
            FrameWriter frameWriter,
            ChannelForcer channelForcer,
            StorageLineage storageLineage
    ) {
        this(
                walDirectory,
                segmentTargetBytes,
                SegmentedFileWriteAheadLog::promoteSegment,
                frameWriter,
                SegmentedFileWriteAheadLog::openAppendChannel,
                DirectoryDurability::forceParent,
                storageLineage
        );
        this.channelForcer = Objects.requireNonNull(
                channelForcer,
                "channelForcer"
        );
    }

    SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes,
            SegmentPromoter segmentPromoter,
            FrameWriter frameWriter,
            ActiveSegmentOpener activeSegmentOpener,
            DirectoryForcer directoryForcer
    ) {
        this(
                walDirectory,
                segmentTargetBytes,
                segmentPromoter,
                frameWriter,
                activeSegmentOpener,
                directoryForcer,
                null
        );
    }

    private SegmentedFileWriteAheadLog(
            Path walDirectory,
            long segmentTargetBytes,
            SegmentPromoter segmentPromoter,
            FrameWriter frameWriter,
            ActiveSegmentOpener activeSegmentOpener,
            DirectoryForcer directoryForcer,
            StorageLineage configuredLineage
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
        this.directoryForcer =
                Objects.requireNonNull(
                        directoryForcer,
                        "directoryForcer"
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
        this.frameCodec = new WalFrameCodec();
        this.logicalEntries = new ArrayList<>();

        this.storageLineage =
                resolveStorageLineage(
                        configuredLineage
                );

        this.initializer =
                new WalSegmentInitializer(
                        storageLineage
                );

        initialize();
    }

    @Override
    public synchronized void append(WalRecord record) {
        appendLocal(1, List.of(record));
    }

    @Override
    public synchronized AppendBatchResult appendLocal(
            long term,
            List<WalRecord> records
    ) {
        ensureWritable();
        if (term <= 0) {
            throw new IllegalArgumentException("term must be positive");
        }
        Objects.requireNonNull(records, "records");
        if (records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }

        long nextIndex = localDurableIndex() + 1;
        List<LogEntry> entries = new ArrayList<>(records.size());
        for (WalRecord record : List.copyOf(records)) {
            entries.add(new LogEntry(nextIndex++, term, record));
        }
        return appendValidated(entries, 0);
    }

    @Override
    public synchronized AppendBatchResult appendReplicated(
            LogPoint previous,
            List<LogEntry> suppliedEntries
    ) {
        ensureWritable();
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(suppliedEntries, "entries");
        List<LogEntry> entries = List.copyOf(suppliedEntries);
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }
        long expectedFirst = previous.logIndex() + 1;
        if (entries.getFirst().logIndex() != expectedFirst) {
            throw new LogGapException(
                    entries.getFirst().logIndex(),
                    expectedFirst
            );
        }

        validatePrevious(previous);
        int alreadyPresent = validateReplicationEntries(entries);
        return appendValidated(
                entries.subList(alreadyPresent, entries.size()),
                alreadyPresent,
                entries.getFirst().logIndex()
        );
    }

    @Override
    public synchronized List<LogEntry> readFrom(
            long firstIndex,
            int maximumEntries
    ) {
        ensureOpen();
        if (firstIndex <= 0) {
            throw new IllegalArgumentException("firstIndex must be positive");
        }
        if (maximumEntries <= 0) {
            throw new IllegalArgumentException("maximumEntries must be positive");
        }

        long lastIndex = localDurableIndex();
        if (logicalEntries.isEmpty()) {
            if (firstIndex == lastIndex + 1) {
                return List.of();
            }
            throw new LogGapException(firstIndex, lastIndex + 1);
        }

        long firstRetained = logicalEntries.getFirst().logIndex();
        if (firstIndex < firstRetained) {
            throw new HistoryReclaimedException(snapshotBoundary.logIndex());
        }
        if (firstIndex > lastIndex + 1) {
            throw new LogGapException(firstIndex, lastIndex + 1);
        }
        if (firstIndex == lastIndex + 1) {
            return List.of();
        }

        int from = Math.toIntExact(firstIndex - firstRetained);
        int to = Math.min(logicalEntries.size(), from + maximumEntries);
        return List.copyOf(logicalEntries.subList(from, to));
    }

    @Override
    public synchronized Optional<LogEntry> entry(long index) {
        ensureOpen();
        if (index <= 0 || logicalEntries.isEmpty()) {
            return Optional.empty();
        }
        long first = logicalEntries.getFirst().logIndex();
        long offset = index - first;
        if (offset < 0 || offset >= logicalEntries.size()) {
            return Optional.empty();
        }
        return Optional.of(logicalEntries.get(Math.toIntExact(offset)));
    }

    @Override
    public synchronized LogPoint lastLogPoint() {
        ensureOpen();
        return logicalEntries.isEmpty()
                ? snapshotBoundary
                : logicalEntries.getLast().point();
    }

    @Override
    public synchronized long localDurableIndex() {
        ensureOpen();
        return logicalEntries.isEmpty()
                ? snapshotBoundary.logIndex()
                : logicalEntries.getLast().logIndex();
    }

    @Override
    public synchronized void restoreSnapshotBoundary(LogPoint boundary) {
        ensureOpen();
        Objects.requireNonNull(boundary, "boundary");
        if (!snapshotBoundary.equals(LogPoint.EMPTY)
                && !snapshotBoundary.equals(boundary)) {
            throw new LogConflictException(
                    "A different snapshot boundary is already installed"
            );
        }
        if (!logicalEntries.isEmpty()) {
            long firstRetained = logicalEntries.getFirst().logIndex();
            Optional<LogEntry> boundaryEntry = entry(boundary.logIndex());
            if (boundaryEntry.isPresent()
                    && boundaryEntry.get().logTerm() != boundary.logTerm()) {
                throw new LogConflictException(
                        "Snapshot term does not match retained WAL boundary"
                );
            }
            if (boundaryEntry.isEmpty()
                    && firstRetained != boundary.logIndex() + 1) {
                throw new LogConflictException(
                        "Snapshot boundary does not precede retained WAL history"
                );
            }
        }
        snapshotBoundary = boundary;
    }

    @Override
    public synchronized LogPoint snapshotBoundary() {
        ensureOpen();
        return snapshotBoundary;
    }

    @Override
    public StorageLineage lineage() {
        return storageLineage;
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
    public StorageLineage storageLineage() {
        return storageLineage;
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

            recoverAndValidateLogicalEntries(segments);
            directoryForcer.force(
                    segments.getLast().path()
            );
            openActiveSegment(segments.getLast());

        } catch (IOException e) {
            throw new WalException(
                    "Failed to initialize segmented WAL: "
                            + walDirectory,
                    e
            );
        }
    }

    private StorageLineage resolveStorageLineage(
            StorageLineage configuredLineage
    ) {
        try {
            Files.createDirectories(
                    walDirectory
            );

            List<WalSegment> segments =
                    discovery.discover(
                            walDirectory
                    );

            if (segments.isEmpty()) {
                return configuredLineage == null
                        ? StorageLineage.create()
                        : configuredLineage;
            }

            StorageLineage persisted =
                    WalSegmentInitializer.readLineage(
                            segments.getFirst().path()
                    );

            if (configuredLineage != null
                    && !configuredLineage.equals(persisted)) {
                throw new WalException(
                        "Configured storage lineage does not match WAL lineage"
                );
            }

            return persisted;

        } catch (IOException e) {
            throw new WalException(
                    "Failed to resolve WAL storage lineage",
                    e
            );
        }
    }

    /**
     * Reconstructs the runtime logical-log view from the durable segment
     * files while proving that every segment belongs to this WAL and that the
     * retained logical indexes form one consecutive suffix.
     */
    private void recoverAndValidateLogicalEntries(
            List<WalSegment> segments
    ) {
        for (WalSegment segment : segments) {
            initializer.validate(segment.path());
        }

        logicalEntries.clear();
        long expectedIndex = -1;

        for (int index = 0;
             index < segments.size();
             index++) {

            List<LogEntry> recovered = readEntrySegment(
                    segments.get(index),
                    roleOf(index, segments.size())
            );

            for (LogEntry entry : recovered) {
                if (expectedIndex < 0) {
                    expectedIndex = entry.logIndex();
                }
                if (entry.logIndex() != expectedIndex) {
                    throw new WalException(
                            "Non-consecutive WAL log index: expected "
                                    + expectedIndex
                                    + " but found "
                                    + entry.logIndex()
                    );
                }
                logicalEntries.add(entry);
                expectedIndex++;
            }
        }
    }

    private AppendBatchResult appendValidated(
            List<LogEntry> entries,
            int alreadyPresent
    ) {
        long firstIndex = entries.getFirst().logIndex();
        return appendValidated(entries, alreadyPresent, firstIndex);
    }

    private AppendBatchResult appendValidated(
            List<LogEntry> newEntries,
            int alreadyPresent,
            long firstIndex
    ) {
        if (newEntries.isEmpty()) {
            return new AppendBatchResult(
                    firstIndex,
                    localDurableIndex(),
                    0,
                    alreadyPresent,
                    currentDurablePosition()
            );
        }

        List<ByteBuffer> frames = new ArrayList<>(newEntries.size());
        long groupBytes = 0;
        for (LogEntry entry : newEntries) {
            ByteBuffer frame = frameCodec.encode(entry);
            groupBytes += frame.remaining();
            frames.add(frame);
        }

        rotateForGroupIfNeeded(groupBytes);

        try {
            for (ByteBuffer frame : frames) {
                frameWriter.write(activeChannel, frame);
            }
            channelForcer.force(activeChannel, true);
            logicalEntries.addAll(newEntries);

            return new AppendBatchResult(
                    firstIndex,
                    newEntries.getLast().logIndex(),
                    newEntries.size(),
                    alreadyPresent,
                    currentDurablePosition()
            );
        } catch (IOException e) {
            poisoned = true;
            throw new WalException("Failed to append WAL batch", e);
        }
    }

    private void validatePrevious(LogPoint previous) {
        if (!snapshotBoundary.equals(LogPoint.EMPTY)
                && previous.equals(snapshotBoundary)) {
            return;
        }
        if (previous.equals(LogPoint.EMPTY)) {
            if (!logicalEntries.isEmpty()
                    && logicalEntries.getFirst().logIndex() == 1) {
                return;
            }
            if (logicalEntries.isEmpty()) {
                return;
            }
            throw new LogConflictException(
                    "Empty previous point does not match retained WAL prefix"
            );
        }

        LogEntry existing = entry(previous.logIndex()).orElseThrow(
                () -> new LogConflictException(
                        "Previous log index is not retained: "
                                + previous.logIndex()
                )
        );
        if (existing.logTerm() != previous.logTerm()) {
            throw new LogConflictException(
                    "Previous log term mismatch at index "
                            + previous.logIndex()
            );
        }
    }

    private int validateReplicationEntries(List<LogEntry> entries) {
        long expected = entries.getFirst().logIndex();
        long expectedNew = localDurableIndex() + 1;
        int alreadyPresent = 0;

        for (LogEntry candidate : entries) {
            if (candidate.logIndex() != expected++) {
                throw new LogGapException(candidate.logIndex(), expected - 1);
            }

            Optional<LogEntry> existing = entry(candidate.logIndex());
            if (existing.isPresent()) {
                if (!existing.get().equals(candidate)) {
                    throw new LogConflictException(
                            "Conflicting entry at index " + candidate.logIndex()
                    );
                }
                alreadyPresent++;
                continue;
            }

            if (candidate.logIndex() != expectedNew) {
                throw new LogGapException(candidate.logIndex(), expectedNew);
            }
            expectedNew++;
        }
        return alreadyPresent;
    }

    private void rotateForGroupIfNeeded(long groupBytes) {
        try {
            long activeSize = activeChannel.size();
            boolean hasEntries =
                    activeSize > WalSegmentInitializer.WAL_HEADER_SIZE;
            if (hasEntries && activeSize + groupBytes > segmentTargetBytes) {
                rotate();
            }
        } catch (IOException e) {
            throw new WalException("Failed to inspect active WAL segment size", e);
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
        directoryForcer.force(path);
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

            directoryForcer.force(destination);

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
        return readEntrySegment(segment, role)
                .stream()
                .map(LogEntry::record)
                .toList();
    }

    private List<LogEntry> readEntrySegment(
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
            ).stream().map(LogEntry::record).toList();

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

    private List<LogEntry> readFrames(
            FileChannel channel,
            WalSegment segment,
            SegmentRole role
    ) throws IOException {
        List<LogEntry> records =
                new ArrayList<>();

        while (true) {
            DecodedFrame frame =
                    frameCodec.readNext(channel);

            switch (frame.status()) {
                case COMPLETE ->
                        records.add(frame.entry());

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

    @FunctionalInterface
    interface DirectoryForcer {

        void force(
                Path publishedPath
        ) throws IOException;
    }

    @FunctionalInterface
    interface ChannelForcer {
        void force(FileChannel channel, boolean metadata)
                throws IOException;
    }
}
