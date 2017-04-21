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
package org.apache.beam.sdk.io.gcp;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.util.ApiSurface.classesInPackage;
import static org.apache.beam.sdk.util.ApiSurface.containsOnlyClassesMatching;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.beam.sdk.util.ApiSurface;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** API surface verification for {@link org.apache.beam.sdk.io.gcp}. */
@RunWith(JUnit4.class)
public class GcpApiSurfaceTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder(new File("/tmp/"));

  private static final Set<String> ignoredPackages =
      ImmutableSet.of("org.apache.beam", "org.joda.time", "com.fasterxml.jackson", "java", "javax");

  @Test
  public void testGcpApiSurface() throws Exception {

    final Package thisPackage = getClass().getPackage();
    final ClassLoader thisClassLoader = getClass().getClassLoader();

    final ApiSurface apiSurface =
        ApiSurface.ofPackage(thisPackage, thisClassLoader)
            .pruningPattern("org[.]apache[.]beam[.].*Test.*")
            .pruningPattern("org[.]apache[.]beam[.].*IT")
            .pruningPattern("java[.]lang.*")
            .pruningPattern("java[.]util.*");
    SortedSet<Class<?>> exposed =
        new TreeSet<>(
            new Comparator<Class<?>>() {
              @Override
              public int compare(Class<?> left, Class<?> right) {
                return left.getCanonicalName().compareTo(right.getCanonicalName());
              }
            });
    exposed.addAll(apiSurface.getExposedClasses());
    File file = temp.newFile("jars_we_care_about");
    System.out.println("Writing to " + file);
    try ( //FileWriter justClassesWriter = new FileWriter(temp.newFile("exposed_classes"));
    //        FileWriter classesInJarWriter = new FileWriter(temp.newFile("exposed_classes_which_jar"));
    FileWriter jarsWeCareAbout = new FileWriter(file)) {
      //      classesInJarWriter.write("Exposed classes in " + thisPackage + "\n");
      //      classesInJarWriter.write("Ignoring [" + Joiner.on(", ").join(ignoredPackages) + "]\n");
      //      classesInJarWriter.write("==========================\n");
      jarsWeCareAbout.write("Jars which exposed classes in " + thisPackage + ", ");
      jarsWeCareAbout.write("Ignoring [" + Joiner.on(", ").join(ignoredPackages) + "]\n");
      jarsWeCareAbout.write("==========================\n");
      //      justClassesWriter.write("Exposed classes in " + thisPackage + "\n");
      //      justClassesWriter.write("Ignoring [" + Joiner.on(", ").join(ignoredPackages) + "]\n");
      //      justClassesWriter.write("==========================\n");
      Multimap<String, Class<?>> jarToClasses = HashMultimap.create();
      for (Class<?> exposedClass : exposed) {
        boolean ignored = false;
        for (String ignoredPackage : ignoredPackages) {
          if (exposedClass.getPackage().getName().startsWith(ignoredPackage)) {
            ignored = true;
            break;
          }
        }
        if (ignored) {
          continue;
        }
        //        justClassesWriter.write(exposedClass.getCanonicalName());
        String mvnSpec = mvnSpecForClass(exposedClass);
        jarToClasses.put(mvnSpec, exposedClass);
      }
      //      justClassesWriter.write("==========================\n");
      //      for (Entry<String, Collection<Class<?>>> jarToClassEntry : jarToClasses.asMap().entrySet()) {
      //        String classLocation = jarToClassEntry.getKey();
      //        classesInJarWriter.write(
      //            String.format(
      //                "%s exposes %s%n", classLocation, jarToClassEntry.getValue()));
      //      }
      jarsWeCareAbout.write("[" + Joiner.on(", ").join(jarToClasses.keySet()) + "]\n\n");
      jarsWeCareAbout.write(Joiner.on("\n").join(jarToClasses.keySet()));
    }
//    Thread.sleep(100000L);

    @SuppressWarnings("unchecked")
    final Set<Matcher<Class<?>>> allowedClasses =
        ImmutableSet.of(
            classesInPackage("com.google.api.client.googleapis"),
            classesInPackage("com.google.api.client.http"),
            classesInPackage("com.google.api.client.json"),
            classesInPackage("com.google.api.client.util"),
            classesInPackage("com.google.api.services.bigquery.model"),
            classesInPackage("com.google.auth"),
            classesInPackage("com.google.bigtable.v2"),
            classesInPackage("com.google.cloud.bigtable.config"),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableClusterName.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableInstanceName.class),
            Matchers.<Class<?>>equalTo(com.google.cloud.bigtable.grpc.BigtableTableName.class),
            classesInPackage("com.google.datastore.v1"),
            classesInPackage("com.google.protobuf"),
            classesInPackage("com.google.type"),
            classesInPackage("com.fasterxml.jackson.annotation"),
            classesInPackage("com.fasterxml.jackson.core"),
            classesInPackage("com.fasterxml.jackson.databind"),
            classesInPackage("io.grpc"),
            classesInPackage("java"),
            classesInPackage("javax"),
            classesInPackage("org.apache.beam"),
            classesInPackage("org.apache.commons.logging"),
            // via Bigtable
            classesInPackage("org.joda.time"));

    assertThat(apiSurface, containsOnlyClassesMatching(allowedClasses));
  }

  private String mvnSpecForClass(Class<?> exposedClass) {
    URL classLocation =
        exposedClass.getResource("/" + exposedClass.getName().replace(".", "/") + ".class");
    String jarPath =
        classLocation.toString().substring(0, classLocation.toString().lastIndexOf("!"));
    checkArgument(jarPath.substring(jarPath.length() - 4).equals(".jar"));
    String repositoryStr = ".m2/repository/";
    int repositoryStrInd = jarPath.lastIndexOf(repositoryStr);
    checkArgument(repositoryStrInd > 0);
    String jarSpec = jarPath.substring(repositoryStrInd + repositoryStr.length());
    String[] split = jarSpec.split("/");
    String jarName = split[split.length - 1];
    String version = split[split.length - 2];
    String artifactId = split[split.length - 3];
    String[] groupIdPart = new String[split.length - 3];
    System.arraycopy(split, 0, groupIdPart, 0, split.length - 3);
    String groupId = Joiner.on('.').join(groupIdPart);
    return String.format("%s:%s:%s", groupId, artifactId, version);
  }
}
