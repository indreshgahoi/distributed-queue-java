package io.github.indreshgahoi.queue.storage.wal;

import io.github.indreshgahoi.queue.storage.replication.LogEntry;
record DecodedFrame(
        FrameReadStatus status,
        LogEntry entry,
        long frameStart
) {

    DecodedFrame {
        if (status == null) {
            throw new NullPointerException("status");
        }

        if (frameStart < 0) {
            throw new IllegalArgumentException(
                    "frameStart must not be negative"
            );
        }

        if (status == FrameReadStatus.COMPLETE
                && entry == null) {

            throw new IllegalArgumentException(
                    "COMPLETE frame must contain a LogEntry"
            );
        }

        if (status != FrameReadStatus.COMPLETE
                && entry != null) {

            throw new IllegalArgumentException(
                    "Incomplete frame must not contain a LogEntry"
            );
        }
    }

    static DecodedFrame complete(
            long frameStart,
            LogEntry entry
    ) {
        return new DecodedFrame(
                FrameReadStatus.COMPLETE,
                entry,
                frameStart
        );
    }

    static DecodedFrame cleanEof(
            long frameStart
    ) {
        return new DecodedFrame(
                FrameReadStatus.CLEAN_EOF,
                null,
                frameStart
        );
    }

    static DecodedFrame tornLength(
            long frameStart
    ) {
        return new DecodedFrame(
                FrameReadStatus.TORN_LENGTH,
                null,
                frameStart
        );
    }

    static DecodedFrame tornPayload(
            long frameStart
    ) {
        return new DecodedFrame(
                FrameReadStatus.TORN_PAYLOAD,
                null,
                frameStart
        );
    }

    static DecodedFrame tornChecksum(
            long frameStart
    ) {
        return new DecodedFrame(
                FrameReadStatus.TORN_CHECKSUM,
                null,
                frameStart
        );
    }
}
