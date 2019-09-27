package main.java.com.dote.ksql.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.lang.*;

/**
 * This class implements a user defined function to score a post based on views and previews in 
 * order to inform ranking of a content feed.
 * 
 * It prioritizes new posts by boosting it's score with a parameterized number of points that 
 * decays with previews. Specifically, a "smoothing" sigmoidal weighting function is used whose inflection point is 
 * based on a multiplicative factor of the expected number of previews for a post to generate
 * it's first views.
 * 
 * It also adds to the post score based on the "click through rate", which is used as a heuristic
 * for the goodness/quality of the post whose inflection point is based off of 
 * the average "click through rate" of new posts.
 * 
 * Ref Tutorial for KSQL UDF: 
 *      https://docs.confluent.io/current/ksql/docs/developer-guide/implement-a-udf.html#implement-a-udf
 */
@UdfDescription(name = "RTScoring", description = "Generates a score based on previews and views")
public class RTScoring {
    // TODO: Run Batch analytics to determine historical averages
    // filter by post performance in it's first week.
    /**
     * Parameters of the user defined function determined by historical statistics.
     */
    private final double AVERAGE_CLICK_THRU_RATE = 0.12; 
    private final int EXPECTED_VIEWS_FACTOR = 2;
    private final double STDDEV_CLICK_THRU_RATE = 0.05; // spoofed value
    private final int MAX_COLDSTART_POINTS = 50;
    private final int MAX_HOTNESS_POINTS = 50;

    private final double expected_clicks_to_first_view = 1 / AVERAGE_CLICK_THRU_RATE;
    private final double previews_target = expected_clicks_to_first_view * EXPECTED_VIEWS_FACTOR;
    // TODO: clean this up later if you only want to support longs
//   @Udf(description = "Score post based on parameters previews, views, in format INTs. Non-nullable")
//   public double rtscore(final int v1, final int v2) {
//     return v1 * v2;
//   }

/**
 * Returns a score for a post to be used in ranking posts.
 * 
 * @param previews number of times post has been shown in user's feeds
 * @param views number of times user has clicked on post in feed
 * @return a score in range [0.0, 100.0] to be used for ranking posts among each other in feed generation.
 */
  @Udf(description = "Score post based on parameters previews, views, in format BIGINTs. Non-nullable")
  public double rtscore(final long previews, final long views) {
      /**
       * Assigns score based on max points allowed for each "feature", weighted by a measure of it's 
       * intensity.
       */
    final double points_for_hotness = MAX_HOTNESS_POINTS * hotness_weight(previews, views);
    final double points_for_coldstart = MAX_COLDSTART_POINTS * coldness_weight(previews);
    return points_for_hotness + points_for_coldstart;
  }

  /**
   * Returns a weighting factor that measures the coldness of a post.
   * 
   * @param previews
   * @return a weight between [0.0, 1.0]
   */
  private double coldness_weight(final long previews) {
      /**
       * sigmoid visualization: https://www.wolframalpha.com/input/?i=%281-1%2F%281%2Be%5E%28+-0.5%28x+-+20%29%29%29%29+from+0+to+30
       */
      final double sigmoid_steepness_factor = 0.5;

      return 1.0 / (1.0 + Math.exp(-sigmoid_steepness_factor * (previews - previews_target)));
  }

  /**
   * Returns a weighting factor that measures the hotness of a post.
   * 
   * @param previews
   * @param views
   * @return a weight between [0.0, 1.0]
   */
  private double hotness_weight(final long previews, final long views) {
      /**
       * sigmoid visualization: https://www.wolframalpha.com/input/?i=1%2F%281%2Be%5E%28-20*+%28x+-+0.11%29%29%29+from+0+to+1
       */
      final double click_thru_rate = 1.0 * previews / (previews + views);
      final double sigmoid_steepness_factor = 20.0;

      return 1.0 / (1.0 + Math.exp(-sigmoid_steepness_factor * (click_thru_rate - AVERAGE_CLICK_THRU_RATE)));
}

// TODO: Clean this up if you end up not using this.
//   @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
//   public Double multiply(final Long v1, final Long v2) {
//     return v1 == null || v2 == null ? null : v1 * v2;
//   }

//   @Udf(description = "multiply two non-nullable DOUBLEs.")
//   public Double multiply(final double v1, double v2) {
//     return v1 * v2;
//   }
}
