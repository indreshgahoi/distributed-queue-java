package io.github.indreshgahoi.queue.gateway.application.port.out;

import io.github.indreshgahoi.queue.gateway.domain.model.ForwardRequest;
import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;
import io.github.indreshgahoi.queue.gateway.domain.model.QueueRoute;

public interface QueueNodeClient {
    ForwardResponse forward(QueueRoute route, ForwardRequest request);
}
