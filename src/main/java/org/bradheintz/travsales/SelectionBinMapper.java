/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 *
 * @author bradheintz
 */
public class SelectionBinMapper extends Mapper<LongWritable, Text, VIntWritable, Text> {
    private final static Logger log = Logger.getLogger(SelectionBinMapper.class);

    private int numBins = 10;

    private Random random = new Random();

    private VIntWritable outKey = new VIntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        outKey.set(random.nextInt(numBins));
        context.write(outKey, value); // shuffle
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        numBins = context.getConfiguration().getInt("numberOfSelectionBins", 10);
    }

}
