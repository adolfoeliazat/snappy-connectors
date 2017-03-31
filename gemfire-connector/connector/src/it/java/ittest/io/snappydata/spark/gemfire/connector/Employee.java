/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ittest.io.snappydata.spark.gemfire.connector;

import java.io.Serializable;

public class Employee implements Serializable {

  private String name;

  private int age;

  public Employee(String n, int a) {
    name = n;
    age = a;
  }

  public String getName() {
    return name;
  }

  public int getAge() {
    return age;
  }

  public String toString() {
    return new StringBuilder().append("Employee[name=").append(name).
        append(", age=").append(age).
        append("]").toString();
  }

  public boolean equals(Object o) {
    if (o instanceof Employee) {
      return ((Employee)o).name.equals(name) && ((Employee)o).age == age;
    }
    return false;
  }

}

