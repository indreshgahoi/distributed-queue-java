package io.github.indreshgahoi.queue.storage.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class WalSegmentReclaimer {

    private final Path walDirectory;
    private final WalSegmentDiscovery discovery;
    private final SegmentDeleter deleter;

    public WalSegmentReclaimer(
            Path walDirectory
    ) {
        this(walDirectory, Files::delete);
    }

    WalSegmentReclaimer(
            Path walDirectory,
            SegmentDeleter deleter
    ) {
        this.walDirectory =
                Objects.requireNonNull(
                        walDirectory,
                        "walDirectory"
                );

        this.discovery =
                new WalSegmentDiscovery();
        this.deleter = Objects.requireNonNull(
                deleter,
                "deleter"
        );
    }

    public void reclaimBefore(
            long boundarySegmentId
    ) {

        List<WalSegment> segments =
                discovery.discover(
                        walDirectory
                );

        if (segments.isEmpty()) {
            return;
        }

        long activeSegmentId =
                segments.getLast()
                        .segmentId();

        if (boundarySegmentId
                > activeSegmentId) {

            throw new WalException(
                    "Compaction boundary "
                            + boundarySegmentId
                            + " crosses active WAL segment "
                            + activeSegmentId
            );
        }

        for (WalSegment segment : segments) {

            if (segment.segmentId()
                    >= boundarySegmentId) {
                continue;
            }

            deleteSegment(
                    segment
            );
        }
    }

    private void deleteSegment(
            WalSegment segment
    ) {

        try {
            deleter.delete(
                    segment.path()
            );

        } catch (IOException e) {
            throw new WalException(
                    "Failed to reclaim WAL segment "
                            + segment.segmentId(),
                    e
            );
        }
    }

    @FunctionalInterface
    interface SegmentDeleter {
        void delete(Path path) throws IOException;
    }
}
