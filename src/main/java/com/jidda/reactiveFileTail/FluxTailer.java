package com.jidda.reactiveFileTail;

import org.apache.commons.io.input.Tailer;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;


public class FluxTailer {

    private static final Logger LOGGER = Loggers.getLogger(com.jidda.reactiveFileTail.FluxTailer.class);

    private DirectProcessor<String> processor;
    private FluxSink<String> sink;
    private Tailer tailer;
    private final File file;
    private final Duration pollingInterval;
    private final AtomicBoolean isStarted;
    private final AtomicBoolean fromEnd;

    public FluxTailer(File file, Duration pollingInterval,boolean fromEnd) {
        this.file = file;
        this.pollingInterval = pollingInterval;
        this.isStarted = new AtomicBoolean(false);
        this.fromEnd = new AtomicBoolean(fromEnd);
    }

    public FluxTailer(File file, Duration pollingInterval) {
        this(file,pollingInterval,false);
    }

    public FluxTailer(String filename, Duration pollingInterval) {
        this(new File(filename), pollingInterval);
    }

    public void readFromStart(){
        this.fromEnd.set(false);
    }

    public void readFromEnd(){
        this.fromEnd.set(true);
    }

    public void start() {
        if(!this.isStarted.get()) {
            this.isStarted.set(true);
            FluxTailerListener listener = new FluxTailerListener();
            this.tailer = new Tailer(file, listener, pollingInterval.toMillis(),fromEnd.get());
            this.processor = DirectProcessor.create();
            this.sink = this.processor.serialize().sink();
            listener.setSink(this.sink);
            Thread tailerThread = new Thread(this.tailer);
            tailerThread.setDaemon(true);
            tailerThread.start();
        }
        else {
            LOGGER.error("FluxTailer is already started",new IllegalStateException());
        }
    }

    public void stop() {
        if(this.isStarted.get()) {
            this.tailer.stop();
            this.sink.complete();
            this.isStarted.set(false);
        }
        else {
            LOGGER.info("FluxTailer has not yet started");
        }
    }

    public Flux<String> flux() {
        return this.processor != null && !this.processor.isTerminated() ? this.processor : Flux.empty();
    }
}
