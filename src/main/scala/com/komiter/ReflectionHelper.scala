// scalastyle:off header
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.komiter

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging


object ReflectionHelper extends Logging {
  import scala.reflect.runtime.universe.runtimeMirror

  private def reflectFieldWithContextClassloaderLoosenType(obj: Any, fieldName: String): Option[Any] = {
    val typeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)
    val instanceMirror = typeMirror.reflect(obj)

    val members = instanceMirror.symbol.typeSignature.members
    val field = members.find(_.name.decodedName.toString == fieldName)
    field match {
      case Some(f) =>
        try {
          Some(instanceMirror.reflectField(f.asTerm).get)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to reflect field $fieldName from $obj. $e")
            None
        }

      case None =>
        logWarning(s"Failed to reflect field $fieldName from $obj.")
        None
    }
  }

  def reflectFieldWithContextClassloader[OUT](obj: Any, fieldName: String): Option[OUT] = {
    reflectFieldWithContextClassloaderLoosenType(obj, fieldName).map(_.asInstanceOf[OUT])
  }

}