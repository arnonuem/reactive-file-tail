package com.jidda.reactiveFileTail;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.io.FileNotFoundException;


class FluxTailerListener extends TailerListenerAdapter {

    private static final Logger LOGGER = Loggers.getLogger(FluxTailerListener.class);

    private FluxSink<String> sink;
    private String file;
    private Tailer tailer;


    FluxTailerListener(){
        this(null);
    }

    FluxTailerListener(FluxSink<String> sink){
        this.sink = sink;
        this.file = "Unknown";
    }

    public void setSink(FluxSink<String> sink) {
        this.sink = sink;
    }

    @Override
    public void init(Tailer tailer) {
        this.tailer = tailer;
        this.file = tailer.getFile().getAbsolutePath();
    }

    @Override
    public void fileNotFound() {
        if(sink == null){
            throw new IllegalStateException("Need to assign fluxSink");
        }
        sink.error(new FileNotFoundException());
        tailer.stop();
    }

    @Override
    public void fileRotated() {
        LOGGER.info("{} rotated",file);
    }

    @Override
    public void handle(String line) {
        if(sink == null){
            throw new IllegalStateException("Need to assign fluxSink");
        }
        System.out.println("Handling: " + line);
        sink.next(line);
    }

    @Override
    public void handle(Exception ex) {
        if(sink == null){
            throw new IllegalStateException("Need to assign fluxSink");
        }
        sink.error(ex);
        tailer.stop();
    }

}
