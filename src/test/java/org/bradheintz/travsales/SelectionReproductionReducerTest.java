/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.bradheintz.travsales.SelectionReproductionReducer.ScoredChromosome;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author bradheintz
 */
public class SelectionReproductionReducerTest {
    SelectionReproductionReducer reducer = new SelectionReproductionReducer();

    @Test
    public void scoredChromosomeConstruction() {
        Text testText = new Text("9 3 2 7 7 11 13 4 0 4 2 0 7 6 5 4 1 2 1\t18.127488762077725");
        ScoredChromosome sc = new ScoredChromosome(testText);
        Assert.assertEquals(sc.chromosome, "9 3 2 7 7 11 13 4 0 4 2 0 7 6 5 4 1 2 1");
        Assert.assertEquals(sc.score, 18.127488762077725, 0.000000001);
        Assert.assertEquals(sc.accumulatedNormalizedScore, -1.0, 0.0000000001);
    }

    @Test
    public void setGeneOnScoredChromosome() {
        ScoredChromosome sc = new ScoredChromosome(new Text("0 0 0\t1.0"));
        Assert.assertEquals(3, sc.getChromosomeArray().length);
        for (int i = 0; i < 3; ++i) {
            Assert.assertEquals(sc.getChromosomeArray()[i], "0");
        }

        sc.setGene(1, 1);
        Assert.assertEquals(3, sc.getChromosomeArray().length);
        for (int i = 0; i < 3; ++i) {
            Assert.assertEquals(sc.getChromosomeArray()[i], Integer.toString(i % 2));
        }
    }

    @Test
    public void chromosomeSorting() {
        ArrayList<Text> unsortedChromosomes = new ArrayList<Text>(3);
        unsortedChromosomes.add(new Text("z\t 2.0"));
        unsortedChromosomes.add(new Text("b\t 3.0"));
        unsortedChromosomes.add(new Text("q\t 1.0"));

        TreeSet<ScoredChromosome> sortedChromosomes = reducer.getSortedChromosomeSet(unsortedChromosomes); // a little integration-y, since we have to unpack the chromosomes

        Iterator<ScoredChromosome> iter = sortedChromosomes.iterator();
        Assert.assertTrue(iter.hasNext());
        ScoredChromosome sc = iter.next();
        Assert.assertEquals(sc.chromosome, "q");
        Assert.assertEquals(sc.score, 1.0, 0.000001);

        Assert.assertTrue(iter.hasNext());
        sc = iter.next();
        Assert.assertEquals(sc.chromosome, "z");
        Assert.assertEquals(sc.score, 2.0, 0.000001);

        Assert.assertTrue(iter.hasNext());
        sc = iter.next();
        Assert.assertEquals(sc.chromosome, "b");
        Assert.assertEquals(sc.score, 3.0, 0.000001);
        Assert.assertFalse(iter.hasNext());

        Assert.assertEquals(reducer.sideEffectSum, 6.0, 0.000001);
    }

    @Test
    public void scoreNormalization() {
        ArrayList<ScoredChromosome> chromosomes = new ArrayList<ScoredChromosome>(4);
        for (int i = 1; i <= 4; ++i) {
            chromosomes.add(new ScoredChromosome(new Text(String.format("c%d\t%g", i, (double)i))));
            Assert.assertEquals(chromosomes.get(i - 1).accumulatedNormalizedScore, -1.0, 0.000001);
        }
        reducer.sideEffectSum = 10.0;

        reducer.normalizeScores(chromosomes);

        Assert.assertEquals(chromosomes.get(0).accumulatedNormalizedScore, 0.1, 0.000001);
        Assert.assertEquals(chromosomes.get(1).accumulatedNormalizedScore, 0.3, 0.000001);
        Assert.assertEquals(chromosomes.get(2).accumulatedNormalizedScore, 0.6, 0.000001);
        Assert.assertEquals(chromosomes.get(3).accumulatedNormalizedScore, 1.0, 0.000001);
    }
}
