/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 *
 * @author bradheintz
 */
public class ScoringMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static Logger log = Logger.getLogger(ScoringMapper.class);

    private Text outKey = new Text();
    private DoubleWritable outValue = new DoubleWritable();

    protected ChromosomeScorer scorer = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String incomingValue[] = value.toString().split("\t");
        String chromosome = incomingValue[0];
        double score = 0.0;
        if (incomingValue.length > 1) {
            String scoreString = incomingValue[1];
            try {
                score = Double.parseDouble(scoreString);
            } catch(NumberFormatException nfe) {
                // no-op - just go ahead and score it
            }
        } else { // we only go through the scoring process if we don't have a map
            score = scorer.score(value.toString());
        }

        outValue.set(score);
	outKey.set(chromosome);
	context.write(outKey, outValue);
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

	if (config.get("cities") == null) throw new InterruptedException("Failure! No city map.");
        scorer = new ChromosomeScorer(config.get("cities"));
	if (scorer.cities.size() < 3) throw new InterruptedException("Failure! Invalid city map.");
    }
}
