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

import org.apache.beam.examples.complete.game.UserScore.GameActionInfo;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.junit.Test;

/**
 * Created by tgroh on 6/29/16.
 */
public class LeaderBoardTest {

  @Test
  public void testGlobalUserLeaderboard() {
    PCollection<GameActionInfo> gameEvents;

    PCollection<KV<String, Integer>> userLeaderboard = gameEvents.apply(new CreateUserLeaderboard());

    PAssert.that(userLeaderboard).overAllPanes(GlobalWindow.INSTANCE).containsAtLeast(foou, baru, bazu);
  }

  @Test
  public void testWindowedTeamLeaderboard() {
    PCollection<GameActionInfo> gameEvents;

    PCollection<KV<String, Integer>> leaderboard = gameEvents.apply(new CreateTeamLeaderboard());

    PAssert.that(leaderboard).inFinalPane(onTimeWindow).containsInAnyOrder(foo, bar, baz);
    PAssert.that(leaderboard)
        .containsAtLeast(spam, ham, eggs)
        .allowing(foospam, barham, bazeggs);

  }
}
