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

import moment from 'moment';
import {ONE_DAY_SECONDS} from 'services/helpers';

export function parseDashboardData(rawData, startTime, duration, pipeline, customApp) {
  let {
    buckets,
    timeArray
  } = setBuckets(startTime, duration);

  let pipelineCount = 0,
      customAppCount = 0;

  rawData.forEach((runInfo) => {
    if (['cdap-data-pipeline', 'cdap-data-streams'].indexOf(runInfo.artifact.name) !== -1) {
      pipelineCount++;

      if (!pipeline) { return; }
    } else {
      customAppCount++;

      if (!customApp) { return; }
    }

    let startTime = getBucket(runInfo.start * 1000);
    let endTime = getBucket(runInfo.end * 1000);

    if (buckets[startTime]) {
      // add start method
      if (runInfo.startMethod === 'manual') {
        buckets[startTime].manual++;
      } else {
        buckets[startTime].schedule++;
      }

      if (runInfo.running && runInfo.start) {
        // aggregate delay
        let delay = runInfo.running - runInfo.start;
        buckets[startTime].delay += delay;
      }
    }

    // add status
    if (buckets[endTime]) {
      if (runInfo.status === 'COMPLETED') {
        buckets[endTime].successful++;
      } else if (runInfo.status === 'FAILED') {
        buckets[endTime].failed++;
      }
    }

    let startIndex = timeArray.indexOf(startTime);
    // if startTime not found, then the program started before the graph
    // so set start index to first bucket
    if (startIndex === -1) {
      startIndex = 0;
    }

    let endIndex = timeArray.indexOf(endTime);
    // if endTime not found, then the program ended after the graph
    // or the program is still running
    if (endIndex === -1) {
      endTime = getBucket(Date.now());
      endIndex = timeArray.indexOf(endTime);
    }

    // add running
    for (let i = startIndex; i <= endIndex; i++) {
      let time = timeArray[i];

      if (buckets[time]) {
        buckets[time].running++;
        buckets[time].runsList.push(runInfo);
      }
    }
  });

  let data = Object.keys(buckets).map((time) => {
    return {
      ...buckets[time],
      time
    };
  });

  return {
    pipelineCount,
    customAppCount,
    data
  };
}

function getBucket(time) {
  if (!time) { return null; }

  return moment(time).startOf('hour').format('x');
}

function setBuckets(startTime, duration) {
  let buckets = {};
  let timeArray = [];

  let start = startTime * 1000;

  // hourly or per 5 minutes
  let numBuckets = duration === ONE_DAY_SECONDS ? 24 : 12;

  for (let i = 0; i < numBuckets; i++) {
    let time = moment(start).startOf('hour');
    if (duration === ONE_DAY_SECONDS) {
      time = time.add(i, 'h').format('x');
    } else {
      time = time.add(i*5, 'm').format('x');
    }

    timeArray.push(time);
    buckets[time] = {
      running: 0,
      successful: 0,
      failed: 0,
      manual: 0,
      schedule: 0,
      delay: 0,
      runsList: []
    };
  }

  return {
    buckets,
    timeArray
  };
}
