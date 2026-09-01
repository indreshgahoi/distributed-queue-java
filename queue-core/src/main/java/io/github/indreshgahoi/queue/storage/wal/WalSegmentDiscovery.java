package io.github.indreshgahoi.queue.storage.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class WalSegmentDiscovery {

    private static final Pattern SEGMENT_PATTERN =
            Pattern.compile(
                    "^segment-(\\d{6})\\.wal$"
            );

    public List<WalSegment> discover(
            Path walDirectory
    ) {
        Objects.requireNonNull(
                walDirectory,
                "walDirectory"
        );

        if (!Files.exists(walDirectory)) {
            return List.of();
        }

        try (Stream<Path> paths =
                     Files.list(walDirectory)) {

            List<WalSegment> segments =
                    paths
                            .filter(Files::isRegularFile)
                            .map(this::toSegmentOrNull)
                            .filter(Objects::nonNull)
                            .sorted(
                                    Comparator.comparingLong(
                                            WalSegment::segmentId
                                    )
                            )
                            .toList();

            validateNoGaps(segments);

            return List.copyOf(segments);

        } catch (IOException e) {
            throw new WalException(
                    "Failed to discover WAL segments in "
                            + walDirectory,
                    e
            );
        }
    }

    public Optional<WalSegment> activeSegment(
            Path walDirectory
    ) {
        List<WalSegment> segments =
                discover(walDirectory);

        if (segments.isEmpty()) {
            return Optional.empty();
        }

        /*
         * Highest authoritative segment ID is active.
         */
        return Optional.of(
                segments.getLast()
        );
    }

    private WalSegment toSegmentOrNull(
            Path path
    ) {
        String filename =
                path.getFileName()
                        .toString();

        Matcher matcher =
                SEGMENT_PATTERN.matcher(
                        filename
                );

        /*
         * Important:
         *
         * .tmp files and unrelated files are simply
         * not authoritative segments.
         */
        if (!matcher.matches()) {
            return null;
        }

        long segmentId =
                Long.parseLong(
                        matcher.group(1)
                );

        return new WalSegment(
                segmentId,
                path
        );
    }

    private void validateNoGaps(
            List<WalSegment> segments
    ) {
        for (int i = 1;
             i < segments.size();
             i++) {

            long previous =
                    segments.get(i - 1)
                            .segmentId();

            long current =
                    segments.get(i)
                            .segmentId();

            if (current != previous + 1) {
                throw new WalException(
                        "Gap in WAL segment sequence: "
                                + previous
                                + " -> "
                                + current
                );
            }
        }
    }
}