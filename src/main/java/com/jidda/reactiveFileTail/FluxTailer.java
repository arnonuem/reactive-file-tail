package com.jidda.reactiveFileTail;

import org.apache.commons.io.input.Tailer;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;
import java.io.File;
import java.time.Duration;


public class FluxTailer{

    private final Logger LOGGER = Loggers.getLogger(FluxTailer.class);

    private DirectProcessor<String> processor;
    private FluxSink<String> sink;
    private final FluxTailerListener listener;
    private final Tailer tailer;

    public FluxTailer(File file, Duration pollingInterval) {
        listener = new FluxTailerListener();
        tailer = new Tailer(
                file,
                listener,
                pollingInterval.toMillis()
        );
    }

    public FluxTailer(String filename, Duration pollingInterval) {
        this(new File(filename),pollingInterval);
    }

    public void start(){
        processor = DirectProcessor.create();
        sink = processor.serialize().sink();
        listener.setSink(sink);
        Thread tailerThread = new Thread(tailer);
        tailerThread.setDaemon(true);
        tailerThread.start();
    }

    public void stop(){
        tailer.stop();
        sink.complete();
    }

    public Flux<String> flux(){
        if(processor == null || processor.isTerminated())
            return Flux.empty();
        return processor;
    }


}
