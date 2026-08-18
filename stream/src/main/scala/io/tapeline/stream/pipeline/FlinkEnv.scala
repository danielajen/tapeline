package io.tapeline.stream.pipeline

import io.tapeline.stream.config.StreamConfig
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/** Builds a configured execution environment.
  *
  * Everything here is a correctness setting rather than a tuning knob, which
  * is why it lives in code and not in flink-conf.yaml: a job that is
  * accidentally deployed without checkpointing still produces output, just
  * without any of the guarantees the rest of this system is built on. State
  * backend and checkpoint storage genuinely are deployment concerns and stay
  * in the cluster config (see deploy/k8s/flink-configmap.yaml).
  */
object FlinkEnv {

  def create(cfg: StreamConfig): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(cfg.parallelism)

    // Defaults to EXACTLY_ONCE. Combined with the transactional Kafka sink,
    // this is what makes the end-to-end guarantee real rather than a claim
    // about the processing layer alone.
    env.enableCheckpointing(cfg.checkpointIntervalMs)

    val ckpt = env.getCheckpointConfig
    ckpt.setCheckpointTimeout(cfg.checkpointTimeoutMs)

    // A minimum pause, not just an interval. Without it, a checkpoint that
    // takes longer than the interval is immediately followed by the next and
    // the job spends all its time checkpointing and none of it processing —
    // which presents as a throughput collapse with no obvious cause.
    ckpt.setMinPauseBetweenCheckpoints(cfg.minPauseBetweenCheckpointsMs)

    ckpt.setMaxConcurrentCheckpoints(1)

    // Tolerate transient checkpoint failures. Failing the job on the first
    // one turns a slow S3 write into an outage.
    ckpt.setTolerableCheckpointFailureNumber(3)

    env
  }
}
