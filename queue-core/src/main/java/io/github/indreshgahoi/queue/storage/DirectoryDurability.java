package io.github.indreshgahoi.queue.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public final class DirectoryDurability {

    private DirectoryDurability() {
    }

    public static void forceParent(
            Path path
    ) throws IOException {
        Objects.requireNonNull(path, "path");

        Path parent =
                path.toAbsolutePath()
                        .getParent();

        if (parent == null) {
            throw new IOException(
                    "Path has no parent directory: " + path
            );
        }

        try (FileChannel channel =
                     FileChannel.open(
                             parent,
                             StandardOpenOption.READ
                     )) {
            channel.force(true);
        }
    }
}
