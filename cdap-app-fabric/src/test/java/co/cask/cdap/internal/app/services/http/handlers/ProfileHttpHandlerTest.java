/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.provision.MockProvisioner;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for profile http handler
 */
public class ProfileHttpHandlerTest extends AppFabricTestBase {
  private static final Set<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableSet.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();

  @Test
  public void testSystemProfiles() throws Exception {
    Assert.assertEquals(Collections.singletonList(Profile.NATIVE), listSystemProfiles(200));

    Profile p1 = new Profile("p1", "label", "desc", EntityScope.SYSTEM,
                             new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    putSystemProfile(p1.getName(), p1, 200);
    Optional<Profile> p1Optional = getSystemProfile(p1.getName(), 200);
    Assert.assertTrue(p1Optional.isPresent());
    Assert.assertEquals(p1, p1Optional.get());

    // check list contains both native and p1
    Set<Profile> expected = new HashSet<>();
    expected.add(Profile.NATIVE);
    expected.add(p1);
    Set<Profile> actual = new HashSet<>(listSystemProfiles(200));
    Assert.assertEquals(expected, actual);
    // check that they're both visible to namespaces
    Assert.assertEquals(expected, new HashSet<>(listProfiles(NamespaceId.DEFAULT, true, 200)));

    // check we can add a profile with the same name in a namespace
    Profile p2 = new Profile(p1.getName(), p1.getLabel(), p1.getDescription(), EntityScope.USER,
                             p1.getProvisioner());
    ProfileId p2Id = NamespaceId.DEFAULT.profile(p2.getName());
    putProfile(p2Id, p2, 200);
    // check that all are visible to the namespace
    expected.add(p2);
    Assert.assertEquals(expected, new HashSet<>(listProfiles(NamespaceId.DEFAULT, true, 200)));
    // check that namespaced profile is not visible in system list
    expected.remove(p2);
    Assert.assertEquals(expected, new HashSet<>(listSystemProfiles(200)));

    disableProfile(p2Id, 200);
    deleteProfile(p2Id, 200);
    disableSystemProfile(p1.getName(), 200);
    deleteSystemProfile(p1.getName(), 200);
  }

  @Test
  public void testListAndGetProfiles() throws Exception {
    // no profile should be there in default namespace
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, false, 200);
    Assert.assertEquals(Collections.emptyList(), profiles);

    // try to list all profiles including system namespace before putting a new one, there should only exist a default
    // profile
    profiles = listProfiles(NamespaceId.DEFAULT, true, 200);
    Assert.assertEquals(Collections.singletonList(Profile.NATIVE), profiles);

    // test get single profile endpoint
    Profile defaultProfile = getProfile(ProfileId.NATIVE, 200).get();
    Assert.assertEquals(Profile.NATIVE, defaultProfile);

    // get a nonexisting profile should get a not found code
    getProfile(NamespaceId.DEFAULT.profile("nonExisting"), 404);
  }

  @Test
  public void testPutAndDeleteProfiles() throws Exception {
    Profile invalidProfile = new Profile("MyProfile", "label", "my profile for testing",
                                         new ProvisionerInfo("nonExisting", PROPERTY_SUMMARIES));
    // adding a profile with non-existing provisioner should get a 400
    putProfile(NamespaceId.DEFAULT.profile(invalidProfile.getName()), invalidProfile, 400);

    // put a profile with the mock provisioner
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
                                   new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    ProfileId expectedProfileId = NamespaceId.DEFAULT.profile(expected.getName());
    putProfile(expectedProfileId, expected, 200);

    // get the profile
    Profile actual = getProfile(expectedProfileId, 200).get();
    Assert.assertEquals(expected, actual);

    // list all profiles, should get 2 profiles
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, true, 200);
    Set<Profile> expectedList = ImmutableSet.of(Profile.NATIVE, expected);
    Assert.assertEquals(expectedList.size(), profiles.size());
    Assert.assertEquals(expectedList, new HashSet<>(profiles));

    // adding the same profile should still succeed
    putProfile(expectedProfileId, expected, 200);

    // get non-existing profile should get a 404
    deleteProfile(NamespaceId.DEFAULT.profile("nonExisting"), 404);

    // delete the profile should fail first time since it is by default enabled
    deleteProfile(expectedProfileId, 409);

    // disable the profile then delete should work
    disableProfile(expectedProfileId, 200);
    deleteProfile(expectedProfileId, 200);

    Assert.assertEquals(Collections.emptyList(), listProfiles(NamespaceId.DEFAULT, false, 200));

    // if given some unrelated json, it should return a 400 instead of 500
    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail test = new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                   new ArrayList<>());
    putProfile(NamespaceId.DEFAULT.profile(test.getName()), test, 400);
  }

  @Test
  public void testEnableDisableProfile() throws Exception {
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
      new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    ProfileId profileId = NamespaceId.DEFAULT.profile(expected.getName());

    // enable and disable a non-existing profile should give a 404
    enableProfile(profileId, 404);
    disableProfile(profileId, 404);

    // put the profile
    putProfile(profileId, expected, 200);

    // by default the status should be enabled
    Assert.assertEquals(ProfileStatus.ENABLED, getProfileStatus(profileId, 200).get());

    // enable it again should give a 409
    enableProfile(profileId, 409);

    // disable should work
    disableProfile(profileId, 200);
    Assert.assertEquals(ProfileStatus.DISABLED, getProfileStatus(profileId, 200).get());

    // disable again should give a 409
    disableProfile(profileId, 409);

    // enable should work
    enableProfile(profileId, 200);
    Assert.assertEquals(ProfileStatus.ENABLED, getProfileStatus(profileId, 200).get());

    // now delete should not work since we have the profile enabled
    deleteProfile(profileId, 409);

    // disable and delete
    disableProfile(profileId, 200);
    deleteProfile(profileId, 200);
  }

  @Test
  public void testNullProvisionerProperty() throws Exception {
    // provide a profile with null provsioner property, it should still succeed
    List<ProvisionerPropertyValue> listWithNull = new ArrayList<>();
    listWithNull.add(null);
    Profile profile = new Profile("ProfileWithNull", "label", "should succeed",
      new ProvisionerInfo(MockProvisioner.NAME, listWithNull));
    putProfile(NamespaceId.DEFAULT.profile(profile.getName()), profile, 200);

    // Get the profile, it should not contain the null value, the property should be an empty list
    Profile actual = getProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200).get();
    Assert.assertNotNull(actual);
    Assert.assertEquals(Collections.EMPTY_SET, actual.getProvisioner().getProperties());

    disableProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200);
    deleteProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200);

    // provide a profile with mixed properties with null, it should still succeed
    List<ProvisionerPropertyValue> listMixed = new ArrayList<>(PROPERTY_SUMMARIES);
    listMixed.addAll(listWithNull);
    profile = new Profile("ProfileMixed", "label", "should succeed",
      new ProvisionerInfo(MockProvisioner.NAME, listMixed));
    putProfile(NamespaceId.DEFAULT.profile(profile.getName()), profile, 200);

    // Get the profile, it should not contain the null value, the property should be all non-null properties in the list
    actual = getProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200).get();
    Assert.assertNotNull(actual);
    Assert.assertEquals(PROPERTY_SUMMARIES, actual.getProvisioner().getProperties());
    disableProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200);
    deleteProfile(NamespaceId.DEFAULT.profile(profile.getName()), 200);
  }
}
