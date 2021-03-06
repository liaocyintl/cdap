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

package co.cask.cdap.internal.provision;

import co.cask.cdap.common.ssh.DefaultSSHSession;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.runtime.spi.ssh.SSHContext;
import co.cask.cdap.runtime.spi.ssh.SSHPublicKey;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Default implementation of {@link SSHContext}.
 */
public class DefaultSSHContext implements SSHContext {

  private final LocationFactory locationFactory;
  private final SecureKeyInfo keyInfo;

  DefaultSSHContext(LocationFactory locationFactory, SecureKeyInfo keyInfo) {
    this.locationFactory = locationFactory;
    this.keyInfo = keyInfo;
  }

  @Override
  public SSHPublicKey getSSHPublicKey() {
    try {
      Location location = locationFactory.create(keyInfo.getKeyDirectory()).append(keyInfo.getPublicKeyFile());
      try (InputStream is = location.getInputStream()) {
        return new SSHPublicKey(keyInfo.getUsername(),
                                new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read public key from "
                                   + keyInfo.getKeyDirectory() + "/" + keyInfo.getPublicKeyFile(), e);
    }
  }

  @Override
  public SSHSession createSSHSession(String host, int port, Map<String, String> configs) throws IOException {
    Location location = locationFactory.create(keyInfo.getKeyDirectory()).append(keyInfo.getPrivateKeyFile());

    SSHConfig config = SSHConfig.builder(host)
      .setPort(port)
      .addConfigs(configs)
      .setUser(keyInfo.getUsername())
      .setPrivateKeySupplier(() -> {
        try {
          return ByteStreams.toByteArray(location::getInputStream);
        } catch (IOException e) {
          throw new RuntimeException("Failed to read private key from " + location, e);
        }
      })
      .build();

    return new DefaultSSHSession(config);
  }
}
