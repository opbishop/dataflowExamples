package com.obishop.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class NetflixPipeline {

    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from("/Users/oli/data/netflix_titles.csv"))
                .apply(MapElements.via(
                        new SimpleFunction<String, String>() {
                            @Override
                            public String apply(String line) {
                                System.out.println(line);
                                return line;
                            }}));

        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

}