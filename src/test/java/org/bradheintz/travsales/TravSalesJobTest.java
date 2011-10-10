package org.bradheintz.travsales;

import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.*;


public class TravSalesJobTest {
  @Test
  public void createMap() {
    ArrayList<double[]> roadmap = null;
    roadmap = TravSalesJob.createMap(1);
    Assert.assertEquals(roadmap.size(), 1);

    roadmap = TravSalesJob.createMap(10);
    Assert.assertEquals(roadmap.size(), 10);

    for (double[] coords : roadmap) {
      Assert.assertEquals(coords.length, 2);
      Assert.assertTrue(coords[0] >= 0.0);
      Assert.assertTrue(coords[0] < 1.0);
      Assert.assertTrue(coords[1] >= 0.0);
      Assert.assertTrue(coords[1] < 1.0);
    }
  }
}
