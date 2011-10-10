/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.bradheintz.travsales;

import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 * @author bradheintz
 */
public class ChromosomeScorerTest {
    ChromosomeScorer scorer = null;

    @BeforeTest
    public void setup() {
        scorer = new ChromosomeScorer("0.0,0.0;0.0,1.0;1.0,1.0");
    }

    @Test
    public void getCitiesFromConfigurationString() {
        ArrayList<double[]> cities = ChromosomeScorer.getCitiesFromString("0.0,0.0;0.0,1.0;1.0,1.0");
        Assert.assertEquals(cities.size(), 3);
        Assert.assertEquals(cities.get(0)[0], 0.0, 0.000001);
        Assert.assertEquals(cities.get(0)[1], 0.0, 0.000001);
        Assert.assertEquals(cities.get(1)[0], 0.0, 0.000001);
        Assert.assertEquals(cities.get(1)[1], 1.0, 0.000001);
        Assert.assertEquals(cities.get(2)[0], 1.0, 0.000001);
        Assert.assertEquals(cities.get(2)[1], 1.0, 0.000001);
    }

    @Test
    public void getRouteFromChromosome() {
        ArrayList<Integer> expectedArray = new ArrayList<Integer>(3);

        for (int i = 0; i < 3; ++i) { expectedArray.add(i); }
        ArrayList<Integer> actualArray = scorer.getRouteFromChromosome("0 0");
        Assert.assertEquals(actualArray.size(), expectedArray.size());
        for (int i = 0; i < 3; ++i) Assert.assertEquals(actualArray.get(i), expectedArray.get(i));

        for (int i = 0; i < 3; ++i) { expectedArray.set(i, 2 - i); }
        actualArray = scorer.getRouteFromChromosome("2 1");
        Assert.assertEquals(actualArray.size(), expectedArray.size());
        for (int i = 0; i < 3; ++i) Assert.assertEquals(actualArray.get(i), expectedArray.get(i));
    }

    @Test
    public void score() {
        double fitness = scorer.score("0 0");
        Assert.assertEquals(fitness, Math.sqrt(2) * 2.0 - 2.0, 0.00001);
    }
}
