/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.List;

/**
 * An implementation of the AbstractXmlKeyValueCartesianProduct operator that takes in the xml document
 * as a String input and outputs the cartesian product as Strings.
 *
 * @since 1.0.1
 */
public class XmlKeyValueStringCartesianProduct extends AbstractXmlKeyValueCartesianProduct<String>
{

  InputSource source = new InputSource();

  @OutputPortFieldAnnotation(name = "output")
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected InputSource getInputSource(String tuple)
  {
    source.setCharacterStream(new StringReader(tuple));
    return source;
  }

  @Override
  protected void processResult(List<String> result, String tuple)
  {
    for (String str : result) {
      output.emit(str);
    }
  }
}
