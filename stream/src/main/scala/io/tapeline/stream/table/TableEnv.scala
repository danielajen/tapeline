package io.tapeline.stream.table

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

/** Single place the Table API is entered.
  *
  * Wrapped rather than called directly so the Table API appears in exactly
  * one import across the codebase. Everything else in this module is
  * DataStream, and keeping the boundary narrow is what makes it obvious that
  * the Table API is used only where Iceberg needs a catalog — not as a second
  * way to express processing logic.
  */
object TableEnv {
  def create(env: StreamExecutionEnvironment): StreamTableEnvironment =
    StreamTableEnvironment.create(env)
}
