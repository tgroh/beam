/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.complete.game;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.complete.game.GameStats.CalculateSpammyUsers;
import com.google.cloud.dataflow.examples.complete.game.GameStats.UserSessionInfoFn;
import com.google.cloud.dataflow.examples.complete.game.UserScore.ExtractAndSumScore;
import com.google.cloud.dataflow.examples.complete.game.UserScore.GameActionInfo;
import com.google.cloud.dataflow.examples.complete.game.UserScore.ParseEventFn;
import com.google.cloud.dataflow.examples.complete.game.utils.WriteWindowedToBigQuery;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests of GameStats.
 * Because the pipeline was designed for easy readability and explanations, it lacks good
 * modularity for testing. See our testing documentation for better ideas:
 * https://cloud.google.com/dataflow/pipelines/testing-your-pipeline.
 */
@RunWith(JUnit4.class)
public class GameStatsTest implements Serializable {

  // User scores
  static final KV<String, Integer>[] USER_SCORES_ARRAY = new KV[] {
    KV.of("Robot-2", 66), KV.of("Robot-1", 116), KV.of("user7_AndroidGreenKookaburra", 23),
    KV.of("user7_AndroidGreenKookaburra", 1),
    KV.of("user19_BisqueBilby", 14), KV.of("user13_ApricotQuokka", 15),
    KV.of("user18_BananaEmu", 25), KV.of("user6_AmberEchidna", 8),
    KV.of("user2_AmberQuokka", 6), KV.of("user0_MagentaKangaroo", 4),
    KV.of("user0_MagentaKangaroo", 3), KV.of("user2_AmberCockatoo", 13),
    KV.of("user7_AlmondWallaby", 15), KV.of("user6_AmberNumbat", 11),
    KV.of("user6_AmberQuokka", 4)
  };

  static final List<KV<String, Integer>> USER_SCORES = Arrays.asList(USER_SCORES_ARRAY);

  // The expected list of 'spammers'.
  static final KV[] SPAMMERS = new KV[] {
      KV.of("Robot-2", 66), KV.of("Robot-1", 116)
    };


  /** Test the calculation of 'spammy users'. */
  @Test
  @Category(RunnableOnService.class)
  public void testCalculateSpammyUsers() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input = p.apply(Create.of(USER_SCORES));
    PCollection<KV<String, Integer>> output = input.apply(new CalculateSpammyUsers());

    // Check the set of spammers.
    DataflowAssert.that(output).containsInAnyOrder(SPAMMERS);

    p.run();
  }

  @Test
  public void testPipeline() {
    TestPipeline p = TestPipeline.create();
    TupleTag<Double> sessionLengthTag = new TupleTag<Double>() {};
    TupleTag<KV<String, Integer>> teamScoreTag = new TupleTag<KV<String, Integer>>() {};

    PipelineTransform transform = new PipelineTransform(sessionLengthTag, teamScoreTag);

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0L), new Instant(2000L));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(2000L), new Instant(4000L));
    PTransformTester.of(transform)
        .withInputs(
            new GameActionInfo("ant", "blue", 3, 1234L),
            new GameActionInfo("ann", "blue", 3, 1248L),
            new GameActionInfo("dave", "red", 9, 2044L))
        .producedAtWatermark(new Instant(4096L))
        .producesOutputInWindow(
            firstWindow, KV.of("blue", 6))
        .producesOutputInWindow(
            secondWindow, KV.of("red", 9))
        .withInputs(new GameActionInfo("dan", "red", 7, 1L))
        .producedAtWatermark(new Instant(4097L))
        .producesOutputInPane(
            firstWindow, 1, KV.of("red", 7))
        .withInputs(new GameActionInfo("alex", "blue", 22, 1872L))
        .producedAtWatermark(new Instant(8000L))
        .emptyInWindow(firstWindow)
        .finish();
  }

  private class PipelineTransform
      extends PTransform<PCollection<GameActionInfo>, PCollectionTuple> {
    private final TupleTag<Double> avgSessionLengthTag;
    private final TupleTag<KV<String, Integer>> teamScoreTag;
    
    public PipelineTransform(
        TupleTag<Double> avgSessionLengthTag, TupleTag<KV<String, Integer>> teamScoreTag) {
      this.avgSessionLengthTag = avgSessionLengthTag;
      this.teamScoreTag = teamScoreTag;
    }

    @Override
    public PCollectionTuple apply(PCollection<GameActionInfo> rawEvents) {
      GameStats.Options options = rawEvents.getPipeline().getOptions().as(GameStats.Options.class);
      // Extract username/score pairs from the event stream
      PCollection<KV<String, Integer>> userEvents =
          rawEvents.apply(
              "ExtractUserScore",
              MapElements.via((GameActionInfo gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore()))
                  .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}));

      // Calculate the total score per user over fixed windows, and
      // cumulative updates for late data.
      final PCollectionView<Map<String, Integer>> spammersView =
          userEvents
              .apply(
                  Window.named("FixedWindowsUser")
                      .<KV<String, Integer>>into(
                          FixedWindows.of(
                              Duration.standardMinutes(options.getFixedWindowDuration()))))

              // Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
              // These might be robots/spammers.
              .apply("CalculateSpammyUsers", new CalculateSpammyUsers())
              // Derive a view from the collection of spammer users. It will be used as a side input
              // in calculating the team score sums, below.
              .apply("CreateSpammersView", View.<String, Integer>asMap());

      // [START DocInclude_FilterAndCalc]
      // Calculate the total score per team over fixed windows,
      // and emit cumulative updates for late data. Uses the side input derived above-- the set of
      // suspected robots-- to filter out scores from those users from the sum.
      // Write the results to BigQuery.
      PCollection<KV<String, Integer>> teamScore =
          rawEvents
              .apply(
                  Window.named("WindowIntoFixedWindows")
                      .<GameActionInfo>into(
                          FixedWindows.of(
                              Duration.standardMinutes(options.getFixedWindowDuration()))))
              // Filter out the detected spammer users, using the side input derived above.
              .apply(
                  ParDo.named("FilterOutSpammers")
                      .withSideInputs(spammersView)
                      .of(
                          new DoFn<GameActionInfo, GameActionInfo>() {
                            @Override
                            public void processElement(ProcessContext c) {
                              // If the user is not in the spammers Map, output the data element.
                              if (c.sideInput(spammersView).get(c.element().getUser().trim())
                                  == null) {
                                c.output(c.element());
                              }
                            }
                          }))
              // Extract and sum teamname/score pairs from the event data.
              .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
      // [END DocInclude_FilterAndCalc]

      // [START DocInclude_SessionCalc]
      // Detect user sessions-- that is, a burst of activity separated by a gap from further
      // activity. Find and record the mean session lengths.
      // This information could help the game designers track the changing user engagement
      // as their set of games changes.
      PCollection<Double> avgSessionLengths =
          userEvents
              .apply(
                  Window.named("WindowIntoSessions")
                      .<KV<String, Integer>>into(
                          Sessions.withGapDuration(
                              Duration.standardMinutes(options.getSessionGap())))
                      .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()))
              // For this use, we care only about the existence of the session, not any particular
              // information aggregated over it, so the following is an efficient way to do that.
              .apply(Combine.perKey(x -> 0))
              // Get the duration per session.
              .apply("UserSessionActivity", ParDo.of(new GameStats.UserSessionInfoFn()))
              // [END DocInclude_SessionCalc]
              // [START DocInclude_Rewindow]
              // Re-window to process groups of session sums according to when the sessions complete.
              .apply(
                  Window.named("WindowToExtractSessionMean")
                      .<Integer>into(
                          FixedWindows.of(
                              Duration.standardMinutes(options.getUserActivityWindowDuration()))))
              // Find the mean session duration in each window.
              .apply(Mean.<Integer>globally().withoutDefaults());
      
      return PCollectionTuple.of(teamScoreTag, teamScore)
          .and(avgSessionLengthTag, avgSessionLengths);
    }
  }
}
