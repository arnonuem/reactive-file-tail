package com.jidda.reactiveFileTail;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.FileNotFoundException;

class FluxTailerListener extends TailerListenerAdapter {
    private static final Logger LOGGER = Loggers.getLogger(com.jidda.reactiveFileTail.FluxTailerListener.class);
    private FluxSink<String> sink;
    private String file;
    private Tailer tailer;

    FluxTailerListener() {
        this(null);
    }

    FluxTailerListener(FluxSink<String> sink) {
        this.sink = sink;
        this.file = "Unknown";
    }

    public void setSink(FluxSink<String> sink) {
        this.sink = sink;
    }

    public void init(Tailer tailer) {
        this.tailer = tailer;
        this.file = tailer.getFile().getAbsolutePath();
    }

    public void fileNotFound() {
        if (this.sink == null) {
            throw new IllegalStateException("Need to assign fluxSink");
        } else {
            this.sink.error(new FileNotFoundException());
            this.tailer.stop();
        }
    }

    public void fileRotated() {
        LOGGER.info("{} rotated", this.file);
    }

    public void handle(String line) {
        if (this.sink == null) {
            throw new IllegalStateException("Need to assign fluxSink");
        } else {
            this.sink.next(line);
        }
    }

    public void handle(Exception ex) {
        if (this.sink == null) {
            throw new IllegalStateException("Need to assign fluxSink");
        } else {
            this.sink.error(ex);
            this.tailer.stop();
        }
    }
}
