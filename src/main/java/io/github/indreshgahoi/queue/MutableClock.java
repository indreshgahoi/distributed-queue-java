package io.github.indreshgahoi.queue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

final public class MutableClock  extends Clock {
    private Instant instant;
    private final ZoneId zone;

    MutableClock(Instant instant) {
        this(instant, ZoneOffset.UTC);
    }
    private MutableClock(Instant instant, ZoneId zone) {
        assert instant != null;
        this.instant = instant;
        this.zone = zone;
    }

    void advance(Duration duration) {
        instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
