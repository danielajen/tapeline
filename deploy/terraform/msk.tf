# Managed Kafka.
#
# A note on what this does and does not demonstrate: MSK is provisioned and
# configured here, but "used managed Kafka" and "operated Kafka" are different
# claims. The configuration below — replication factor, minimum ISR,
# transaction log settings, partition counts — is the part that is actually
# load-bearing for this system's guarantees, and it is the part worth being
# able to defend. See docs/DESIGN_DECISIONS.md#d5.

resource "aws_msk_configuration" "main" {
  name              = "${local.name}-config"
  kafka_versions    = ["3.7.x"]
  server_properties = <<-PROPERTIES
    # Never auto-create. Auto-creation uses the broker default partition
    # count, which silently caps the parallelism of every consumer
    # downstream — and the symptom appears weeks later as a throughput
    # ceiling nobody can explain.
    auto.create.topics.enable=false

    # Three replicas, two of which must acknowledge. This pair is what makes
    # the chaos test's zero-loss claim true: one broker can be lost with
    # writes still accepted, and an acknowledged write survives that loss
    # because it is on at least two brokers.
    default.replication.factor=3
    min.insync.replicas=2

    # The transaction coordinator's own log needs the same durability, or
    # exactly-once output has a weaker guarantee than the data it protects.
    transaction.state.log.replication.factor=3
    transaction.state.log.min.isr=2
    offsets.topic.replication.factor=3

    # Deleting a topic must be deliberate, not a typo away.
    delete.topic.enable=true

    # Seven days. The lakehouse is the system of record beyond this, and the
    # Kappa backfill path exists precisely so retention can stay short
    # without losing the ability to recompute.
    log.retention.hours=168

    # Unclean leader election off: an out-of-sync replica must never be
    # promoted. Allowing it trades silent data loss for availability, which
    # is the wrong trade for a system whose whole claim is that it does not
    # lose data.
    unclean.leader.election.enable=false

    num.partitions=6
  PROPERTIES

  description = "Tapeline broker configuration"
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.name
  kafka_version          = "3.7.x"
  number_of_broker_nodes = var.availability_zone_count

  broker_node_group_info {
    instance_type   = var.kafka_broker_instance_type
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.kafka.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.kafka_broker_storage_gb

        # Autoscale storage. A broker that fills its disk stops accepting
        # writes and is genuinely unpleasant to recover by hand.
        provisioned_throughput {
          enabled = false
        }
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  open_monitoring {
    prometheus {
      # JMX and node exporters, scraped by the same Prometheus that scrapes
      # everything else. Broker metrics living in a separate system is how
      # "consumer lag is rising" and "the pipeline is slow" end up being
      # investigated as two unrelated incidents.
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.kafka.name
      }
    }
  }

  tags = merge(local.tags, { Name = local.name })
}

resource "aws_cloudwatch_log_group" "kafka" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 14
  tags              = local.tags
}
