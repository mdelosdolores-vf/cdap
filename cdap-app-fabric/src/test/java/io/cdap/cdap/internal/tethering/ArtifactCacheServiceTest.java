/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

/**
 * Test for Artifact Cache service.
 */
public class ArtifactCacheServiceTest extends AppFabricTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final String PEER_NAME = "peer";

  private CConfiguration cConf;
  private TetheringStore tetheringStore;
  private ArtifactCacheService artifactCacheService;
  private ArtifactRepository artifactRepository;
  private Id.Artifact artifactId;
  private Location appJar;

  private CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.ArtifactCache.ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.setInt(Constants.ArtifactCache.PORT, 11030);
    cConf.set(Constants.ArtifactCache.LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return cConf;
  }

  @Before
  public void setUp() throws Exception {
    cConf = createCConf();
    tetheringStore = getInjector().getInstance(TetheringStore.class);
    ArtifactCache artifactCache = new ArtifactCache(cConf);
    artifactCacheService = new ArtifactCacheService(cConf, artifactCache, tetheringStore, null);
    artifactCacheService.startAndWait();
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
    Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    artifactRepository.addArtifact(artifactId, appJarFile);
    addPeer();
  }

  @After
  public void tearDown() throws Exception {
    artifactCacheService.stopAndWait();
    artifactRepository.deleteArtifact(artifactId);
    appJar.delete();
    deletePeer();
  }

  private byte[] getArtifactBytes(String peerName) throws IOException, URISyntaxException {

    URL url = getURL(peerName);
    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    return httpResponse.getResponseBody();
  }

  private URL getURL(String peerName) throws URISyntaxException, MalformedURLException {
    return getURL(peerName, artifactId);
  }

  private URL getURL(String peerName, Id.Artifact artifactId) throws URISyntaxException, MalformedURLException {
    String urlPath = String.format("/peers/%s/namespaces/%s/artifacts/%s/versions/%s",
                                   peerName, artifactId.toEntityId().getNamespace(),
                                   artifactId.toEntityId().getArtifact(),
                                   artifactId.toEntityId().getVersion());

    return new URI(String.format("http://%s:%d/v3Internal/%s",
                                 cConf.get(Constants.ArtifactCache.ADDRESS),
                                 cConf.getInt(Constants.ArtifactCache.PORT),
                                 urlPath)).toURL();
  }

  private void addPeer() throws PeerAlreadyExistsException, IOException {
    NamespaceAllocation namespaceAllocation = new NamespaceAllocation("default", null,
                                                                      null);
    PeerMetadata peerMetadata = new PeerMetadata(Collections.singletonList(namespaceAllocation),
                                                 Collections.emptyMap(), null);
    PeerInfo peerInfo = new PeerInfo(PEER_NAME, getEndPoint("").toString(),
                                     TetheringStatus.ACCEPTED, peerMetadata, System.currentTimeMillis());
    tetheringStore.addPeer(peerInfo);
  }

  private void deletePeer() throws PeerNotFoundException, IOException {
    tetheringStore.deletePeer(PEER_NAME);
  }

  @Test
  public void testFetchArtifact() throws Exception {
    byte[] artifact = getArtifactBytes(PEER_NAME);
    byte[] expectedBytes = IOUtils.toByteArray(appJar.getInputStream());
    Assert.assertArrayEquals(artifact, expectedBytes);
  }

  @Test
  public void testFetchArtifactUnknownPeer() throws Exception {
    URL url = getURL("unknownpeer");
    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, httpResponse.getResponseCode());
  }

  @Test
  public void testArtifactNotFound() throws Exception {
    Id.Artifact notFoundArtifact = Id.Artifact.from(Id.Namespace.DEFAULT, "other-task", "2.0.0-SNAPSHOT");
    URL url = getURL(PEER_NAME, notFoundArtifact);
    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, httpResponse.getResponseCode());
  }
}
