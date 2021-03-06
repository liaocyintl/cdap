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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import enableSystemApp from 'services/ServiceEnablerUtilities';
import LoadingSVG from 'components/LoadingSVG';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import {myExperimentsApi} from 'api/experiments';

require('./ExperimentsServiceControl.scss');

const PREFIX = 'features.Experiments.ServiceControl';
const MMDSArtifact = 'mmds-app';

export default class ExperimentsServiceControl extends Component {
  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    error: null
  };

  enableMMDS = () => {
    this.setState({
      loading: true
    });
    enableSystemApp({
      shouldStopService: false,
      artifactName: MMDSArtifact,
      api: myExperimentsApi,
      i18nPrefix: ''
    }).subscribe(
      this.props.onServiceStart,
      () => {
        this.setState({
          error: T.translate(`${PREFIX}.errorMessage`),
          loading: false
        });
      }
    );
  };

  renderEnableBtn = () => {
    return (
      <div className="action-container">
        <button
          className="btn btn-primary"
          onClick={this.enableMMDS}
          disabled={this.state.loading}
        >
          {
            this.state.loading ?
              <LoadingSVG />
            :
              null
          }
          <span className="btn-label">{T.translate(`${PREFIX}.enableBtnLabel`)}</span>
        </button>
      </div>
    );
  };

  renderError = () => {
    if (!this.state.error) {
      return null;
    }
    return (
      <div className="experiments-service-control-error">
        <h5 className="text-danger">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{T.translate(`${PREFIX}.errorTitle`)}</span>
        </h5>
        <p className="text-danger">
          {this.state.error}
        </p>
      </div>
    );
  }

  render() {
    return (
      <div className="experiments-service-control">
        <div className="image-containers">
          <img className="img-thumbnail" src="/cdap_assets/img/MMDS_preview1.png" />
          <img className="img-thumbnail" src="/cdap_assets/img/MMDS_preview2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`)} </h2>
          {this.renderEnableBtn()}
          {this.renderError()}
          <p>
            {T.translate(`${PREFIX}.description`)}
          </p>
          <div className="experiments-benefit">
            {T.translate(`${PREFIX}.Benefits.title`)}

            <ul>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b1`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b2`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b3`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b4`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b5`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b6`)}</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}
