package io.github.indreshgahoi.queue.node.application.port.out;

/**
 * Lifecycle boundary for a recovered queue runtime. Keeping this port narrow
 * prevents reconciliation from depending on message operations before the
 * data-plane API and its retry semantics are deliberately introduced.
 */
public interface RuntimeQueue extends AutoCloseable {
    @Override
    void close();
}
