/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.examples.complete.game;

import org.apache.beam.examples.complete.game.LeaderBoard.CalculateTeamScores;
import org.apache.beam.examples.complete.game.LeaderBoard.CalculateUserScores;
import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.runners.direct.CreateStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.collect.ImmutableMap;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LeaderBoard}.
 */
@RunWith(JUnit4.class)
public class LeaderBoardTest {
  private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
  private static final Duration TEAM_WINDOW_DURATION = Duration.standardMinutes(20);

  private enum TestUser {
    RED_ONE("scarlet", "red"),
    RED_TWO("burgundy", "red"),

    BLUE_ONE("navy", "blue"),
    BLUE_TWO("sky", "blue");

    private final String userName;
    private final String teamName;

    TestUser(String userName, String teamName) {
      this.userName = userName;
      this.teamName = teamName;
    }

    public String getUser() {
      return userName;
    }

    public String getTeam() {
      return teamName;
    }
  }

  @Test
  public void testTeamScores() {
    TestPipeline p = TestPipeline.create();

    Instant baseTime = new Instant(0L);
    CreateStream<GameActionInfo> createEvents =
        CreateStream.create(TypeDescriptor.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(event(TestUser.BLUE_ONE, 3, baseTime.plus(Duration.standardSeconds(3))),
                event(TestUser.BLUE_ONE, 2, baseTime.plus(Duration.standardMinutes(1))),
                event(TestUser.BLUE_TWO, 3, baseTime.plus(Duration.standardSeconds(22))),
                event(TestUser.BLUE_TWO, 5, baseTime.plus(Duration.standardMinutes(3))))
            .advanceProcessingTime(Duration.standardMinutes(10))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
            .addElements(event(TestUser.BLUE_ONE, 1, baseTime.plus(Duration.standardMinutes(4))),
                event(TestUser.RED_ONE, 3, baseTime),
                event(TestUser.BLUE_TWO,
                    4,
                    baseTime.plus(TEAM_WINDOW_DURATION).plus(Duration.standardMinutes(1))))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
        .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    PAssert.that(teamScores)
        .inOnTimePane(new IntervalWindow(baseTime, baseTime.plus(TEAM_WINDOW_DURATION)))
        .containsInAnyOrder(KV.of(blueTeam, 14), KV.of(redTeam, 3));
    PAssert.that(teamScores)
        .inOnTimePane(new IntervalWindow(baseTime.plus(TEAM_WINDOW_DURATION),
            baseTime.plus(TEAM_WINDOW_DURATION).plus(TEAM_WINDOW_DURATION)))
        .containsInAnyOrder(KV.of(blueTeam, 4));

    p.run();
  }

  @Test
  public void testTeamScoresLateData() {
    Instant baseTime = new Instant(0L);
    Instant firstWindowCloses = baseTime.plus(ALLOWED_LATENESS).plus(TEAM_WINDOW_DURATION);
    CreateStream<GameActionInfo> createEvents =
        CreateStream.create(TypeDescriptor.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                event(TestUser.BLUE_ONE, 3, baseTime.plus(Duration.standardSeconds(3))),
                event(TestUser.BLUE_TWO, 5, baseTime.plus(Duration.standardMinutes(8))))
            .advanceProcessingTime(Duration.standardMinutes(10))
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
            .addElements(
                event(TestUser.RED_ONE, 3, baseTime.plus(Duration.standardMinutes(1))),
                event(TestUser.RED_ONE, 4, baseTime.plus(Duration.standardMinutes(2))),
                event(TestUser.BLUE_ONE, 3, baseTime.plus(Duration.standardMinutes(5))))
            .advanceWatermarkTo(firstWindowCloses.minus(Duration.standardMinutes(1)))
            // These events are late but should still appear in a late pane
            .addElements(
                event(TestUser.RED_TWO, 2, baseTime),
                event(TestUser.RED_TWO, 5, baseTime.plus(Duration.standardMinutes(1))),
                event(TestUser.RED_TWO, 3, baseTime.plus(Duration.standardMinutes(3))))
            .advanceProcessingTime(Duration.standardMinutes(12))
            .addElements(
                event(TestUser.RED_TWO, 9, baseTime.plus(Duration.standardMinutes(1))),
                event(TestUser.RED_TWO, 1, baseTime.plus(Duration.standardMinutes(3))))
            .advanceWatermarkToInfinity();

    TestPipeline p = TestPipeline.create();
    PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
        .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    BoundedWindow window = new IntervalWindow(baseTime, baseTime.plus(TEAM_WINDOW_DURATION));
    String blueTeam = TestUser.BLUE_ONE.getTeam();
    String redTeam = TestUser.RED_ONE.getTeam();
    PAssert.that(teamScores)
        .inWindow(window)
        .containsInAnyOrder(
            KV.of(redTeam, 7),
            KV.of(redTeam, 17),
            KV.of(redTeam, 27),
            KV.of(blueTeam, 8),
            KV.of(blueTeam, 11));
    PAssert.thatMap(teamScores)
        // The closing behavior of CalculateTeamScores precludes an inFinalPane matcher.
        .inOnTimePane(window)
        .isEqualTo(ImmutableMap.<String, Integer>builder()
            .put(redTeam, 7)
            .put(blueTeam, 11)
            .build());

    p.run();
  }

  @Test
  public void testUserScore() {
    Instant baseTime = new Instant(0);
    CreateStream<GameActionInfo> infos =
        CreateStream.create(TypeDescriptor.of(GameActionInfo.class))
            // Even if user scores arrive late they will be taken into account
            .advanceWatermarkTo(baseTime.plus(ALLOWED_LATENESS).plus(Duration.standardHours(12)))
            .addElements(event(TestUser.BLUE_ONE, 12, baseTime),
                event(TestUser.RED_ONE, 3, baseTime))
            .advanceProcessingTime(Duration.standardMinutes(7))
            .addElements(event(TestUser.RED_ONE, 4, baseTime.plus(Duration.standardMinutes(2))),
                event(TestUser.BLUE_TWO, 3, baseTime),
                event(TestUser.BLUE_ONE, 3, baseTime.plus(Duration.standardMinutes(3))))
            .advanceProcessingTime(Duration.standardMinutes(5))
            .addElements(event(TestUser.RED_ONE, 3, baseTime.plus(Duration.standardMinutes(7))))
            .advanceProcessingTime(Duration.standardMinutes(6))
            .addElements(event(TestUser.BLUE_TWO, 5, baseTime.plus(Duration.standardMinutes(12))))
            .advanceWatermarkToInfinity();

    TestPipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> userScores =
        p.apply(infos).apply(new CalculateUserScores(ALLOWED_LATENESS));

    PAssert.that(userScores)
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(
            KV.of(TestUser.BLUE_ONE.getUser(), 15),
            KV.of(TestUser.RED_ONE.getUser(), 7),
            KV.of(TestUser.RED_ONE.getUser(), 10),
            KV.of(TestUser.BLUE_TWO.getUser(), 3),
            KV.of(TestUser.BLUE_TWO.getUser(), 8));

    p.run();
  }

  private TimestampedValue<GameActionInfo> event(TestUser user, int score, Instant timestamp) {
    return TimestampedValue.of(new GameActionInfo(user.getUser(),
        user.getTeam(),
        score,
        timestamp.getMillis()), timestamp);
  }
}
