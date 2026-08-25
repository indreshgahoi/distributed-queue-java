package io.github.indreshgahoi.queue.storage.wal;

enum FrameReadStatus {

    COMPLETE,

    /*
     * No bytes were found where the next frame would begin.
     *
     * This is normal end-of-file.
     */
    CLEAN_EOF,

    /*
     * Some, but not all, 4 bytes of the frame length
     * were present.
     */
    TORN_LENGTH,

    /*
     * Length prefix was complete, but payload bytes
     * ended before payloadLength was satisfied.
     */
    TORN_PAYLOAD,

    /*
     * Length and payload were complete, but the
     * 4-byte checksum was incomplete.
     */
    TORN_CHECKSUM
}
