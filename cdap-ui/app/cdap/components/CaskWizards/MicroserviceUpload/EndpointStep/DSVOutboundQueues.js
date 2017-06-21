/*
 * Copyright © 2017 Cask Data, Inc.
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
import React from 'react';
import {connect, Provider} from 'react-redux';
import DSVEditor from 'components/DSVEditor';

import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';

const mapStateToOutboundQueuesProps = (state) => {
  return {
    values: state.endpoints.out
  };
};

const mapDispatchToOutboundQueuesProps = (dispatch) => {
  return {
    onChange: (values) => (dispatch({
      type: MicroserviceUploadActions.setOutboundQueues,
      payload: { outboundQueues: values}
    }))
  };
};

const DSVWrapper = connect(
  mapStateToOutboundQueuesProps,
  mapDispatchToOutboundQueuesProps
)(DSVEditor);


export default function DSVOutboundQueues() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <DSVWrapper />
    </Provider>
  );
}
