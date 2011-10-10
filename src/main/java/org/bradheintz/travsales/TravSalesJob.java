/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author bradheintz
 */
public class TravSalesJob extends Configured {

    private static Random random = new Random();

    // LATER these should all be configurable
    private static String popPath = "travsales_populations";
    private static int numCities = 20;
    private static int populationSize = 100000;
    private static int selectionBinSize = 10000;
    private static float topTierToSave = 0.1f; // TODO
    private static float survivorProportion = 0.3f;
    private static float mutationChance = 0.01f;
    // LATER have pluggable strategies, but for now, just pick a number of generations
    private static int generations = 500;


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        FileSystem fs = FileSystem.get(conf);
        String roadmap = createTrivialRoadmap(fs.create(new Path("_CITY_MAP")), conf, numCities);
        conf.set("cities", roadmap);
        System.out.println("city map created...");

        createInitialPopulation(fs.create(new Path(popPath + "/population_0/population_0_init")), populationSize, numCities);
        System.out.println("initial population created...");

        Job job = new Job(conf, "travsales");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(ScoringMapper.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + "/population_0"));
        FileOutputFormat.setOutputPath(job, new Path(popPath + "/population_0_scored"));

        if (!job.waitForCompletion(true)) {
            System.out.println("Failure scoring first generation");
            System.exit(1);
        }

        for (int generation = 0; generation < generations; ++generation) {
            selectAndReproduce(generation, roadmap);
        }
    }


    protected static void selectAndReproduce(int generation, String roadmap) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setFloat("survivorProportion", survivorProportion);
        conf.setFloat("topTierToSave", topTierToSave);
        conf.setInt("selectionBinSize", selectionBinSize);
        conf.setFloat("mutationChance", mutationChance);
        conf.set("cities", roadmap);

        Job job = new Job(conf, String.format("travsales_select_and_reproduce_%d", generation));

        job.setOutputKeyClass(VIntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(TravSalesJob.class);
        job.setMapperClass(SelectionBinMapper.class);
        // job.setCombinerClass(ScoringPassthruReducer.class);
        job.setReducerClass(SelectionReproductionReducer.class);

        FileInputFormat.setInputPaths(job, new Path(popPath + String.format("/population_%d_scored", generation)));
        FileOutputFormat.setOutputPath(job, new Path(popPath + String.format("/population_%d_scored", generation + 1)));

        System.out.println(String.format("Selecting from population %d, breeding and scoring population %d", generation, generation + 1));
        if (!job.waitForCompletion(true)) {
            System.out.println(String.format("FAILURE selecting & reproducing generation %d", generation));
            System.exit(1);
        }
    }


    private static int numSelectionBins() {
        return populationSize / selectionBinSize;
    }

    protected static ArrayList<double[]> createMap(int numCities) {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(numCities);
        for (int i = 0; i < numCities; ++i) {
            double[] coords = {random.nextDouble(), random.nextDouble()};
            roadmap.add(coords);
        }
        return roadmap;
    }

    protected static String createTrivialRoadmap(FSDataOutputStream hdfsOut, Configuration hadoopConfig, final int numCitiesIgnored) throws IOException {
        ArrayList<double[]> roadmap = new ArrayList<double[]>(20);
        for (int i = 0; i < 5; ++i) {
            double dummy = 0.2 * (double)i;
            roadmap.add(new double[] {0.0, dummy});
            roadmap.add(new double[] {dummy, 1.0});
            roadmap.add(new double[] {dummy + 0.2, 0.0});
            roadmap.add(new double[] {1.0, dummy + 0.2});
        }

        StringBuilder configStringBuilder = new StringBuilder("");
        for (int i = 0; i < roadmap.size(); ++i) {
            double[] coords = roadmap.get(i);
            hdfsOut.writeBytes(String.format("%d %g %g\n", i, coords[0], coords[1]));

            if (configStringBuilder.length() > 0) {
                configStringBuilder.append(";");
            }
            configStringBuilder.append(String.format("%g,%g", coords[0], coords[1]));
        }
        hdfsOut.close();
        hdfsOut = null;

        return configStringBuilder.toString();
    }

    protected static String createRoadmap(FSDataOutputStream hdfsOut, Configuration hadoopConfig, final int numCities) throws IOException {
        ArrayList<double[]> roadmap = createMap(numCities);
        StringBuilder configStringBuilder = new StringBuilder("");
        for (int i = 0; i < roadmap.size(); ++i) {
            double[] coords = roadmap.get(i);
            hdfsOut.writeBytes(String.format("%d %g %g\n", i, coords[0], coords[1]));

            if (configStringBuilder.length() > 0) {
                configStringBuilder.append(";");
            }
            configStringBuilder.append(String.format("%g,%g", coords[0], coords[1]));
        }
        hdfsOut.close();
        hdfsOut = null;

        return configStringBuilder.toString();
    }

    protected static void createInitialPopulation(FSDataOutputStream populationOutfile, final int populationSize, final int numCities) throws IOException {
        for (int i = 0; i < populationSize; ++i) {
            for (int j = 0; j < (numCities - 1); ++j) {
                if (j > 0) {
                    populationOutfile.writeBytes(" ");
                }
                populationOutfile.writeBytes(String.format("%d", random.nextInt(numCities - j)));
            }
            populationOutfile.writeBytes("\n");
        }

        populationOutfile.close();
    }
}
