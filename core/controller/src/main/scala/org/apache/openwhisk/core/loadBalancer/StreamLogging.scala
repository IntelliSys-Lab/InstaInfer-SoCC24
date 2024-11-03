package org.apache.openwhisk.core.loadBalancer

import java.io.ByteArrayOutputStream
import java.io.PrintStream

import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.PrintStreamLogging
import java.nio.charset.StandardCharsets

/**
 * Logging facility, that can be used by tests.
 *
 * It contains the implicit Logging-instance, that is needed implicitly for some methods and classes.
 * the logger logs to the stream, that can be accessed from your test, to check if a specific message has been written.
 */
trait StreamLogging {
  lazy val stream = new ByteArrayOutputStream
  lazy val printstream = new PrintStream(stream)
  implicit lazy val logging: Logging = new PrintStreamLogging(printstream)

  def logLines = new String(stream.toByteArray, StandardCharsets.UTF_8).linesIterator.toList
}
