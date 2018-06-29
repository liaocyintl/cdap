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

package co.cask.cdap.metadata;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.metadata.lineage.field.EndPointField;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Mock implementation of the {@link FieldLineageReader} for testing purpose.
 */
public class NoopFieldLineageReader implements FieldLineageReader {
  @Override
  public Set<String> getFields(EndPoint endPoint, long start, long end) {
    return fields();
  }

  public static Set<String> fields() {
    return new HashSet<>(Arrays.asList("name", "address", "address_original", "offset", "body"));
  }

  @Override
  public Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end) {
    return summary();
  }

  @Override
  public Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end) {
    return summary();
  }

  @Override
  public Set<ProgramRunOperations> getIncomingOperations(EndPointField endPointField, long start, long end) {
    return operations();
  }

  @Override
  public Set<ProgramRunOperations> getOutgoingOperations(EndPointField endPointField, long start, long end) {
    return operations();
  }

  private Set<EndPointField> summary() {
    Set<EndPointField> endPointFields = new HashSet<>();
    EndPoint endPoint1 = EndPoint.of("ns", "file");
    EndPoint endPoint2 = EndPoint.of("ns", "anotherfile");
    endPointFields.add(new EndPointField(endPoint1, "a"));
    endPointFields.add(new EndPointField(endPoint1, "b"));
    endPointFields.add(new EndPointField(endPoint1, "c"));
    endPointFields.add(new EndPointField(endPoint2, "x"));
    endPointFields.add(new EndPointField(endPoint2, "y"));
    endPointFields.add(new EndPointField(endPoint2, "z"));
    return endPointFields;
  }

  private Set<ProgramRunOperations> operations() {
    ProgramId program1 = new ProgramId("ns", "app", ProgramType.SPARK, "sparkprogram");
    ProgramId program2 = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "mrprogram");

    EndPoint endPoint1 = EndPoint.of("ns", "file");
    EndPoint endPoint2 = EndPoint.of("ns", "anotherfile");

    ReadOperation read = new ReadOperation("read", "reading file", endPoint1, "offset", "body");
    WriteOperation write = new WriteOperation("write", "writing file", endPoint2, InputField.of("read", "offset"),
            InputField.of("parse", "name"), InputField.of("parse", "address"), InputField.of("parse", "zip"));

    ProgramRunId program1Run1 = program1.run(RunIds.generate(1000));
    ProgramRunId program1Run2 = program1.run(RunIds.generate(2000));
    Set<ProgramRunOperations> programRunOperations = new HashSet<>();
    programRunOperations.add(new ProgramRunOperations(new HashSet<>(Arrays.asList(program1Run1, program1Run2)),
            new HashSet<>(Arrays.asList(read, write))));

    TransformOperation normalize = new TransformOperation("normalize", "normalizing offset",
            Collections.singletonList(InputField.of("read", "offset")), "offset");
    write = new WriteOperation("write", "writing file", endPoint2, InputField.of("normalize", "offset"),
            InputField.of("parse", "name"), InputField.of("parse", "address"), InputField.of("parse", "zip"));

    ProgramRunId program1Run3 = program1.run(RunIds.generate(3000));
    ProgramRunId program1Run4 = program1.run(RunIds.generate(5000));
    ProgramRunId program2Run1 = program2.run(RunIds.generate(4000));
    ProgramRunId program2Run2 = program2.run(RunIds.generate(6000));
    programRunOperations.add(new ProgramRunOperations(new HashSet<>(Arrays.asList(program1Run3,
            program1Run4, program2Run1, program2Run2)), new HashSet<>(Arrays.asList(read, normalize, write))));
    return programRunOperations;
  }
}
