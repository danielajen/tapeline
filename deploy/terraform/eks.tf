resource "aws_iam_role" "eks_cluster" {
  name = "${local.name}-eks-cluster"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "main" {
  name     = local.name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.eks_version

  vpc_config {
    subnet_ids = aws_subnet.private[*].id

    # A public endpoint, restricted by CIDR, rather than a private endpoint
    # plus a bastion. For a portfolio cluster that is the honest tradeoff:
    # a bastion is another instance to pay for and patch. Restrict
    # public_access_cidrs to your own address; the default below does not.
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = ["0.0.0.0/0"]
  }

  # Audit logs are the ones that matter after an incident. The rest are
  # useful during one.
  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  access_config {
    # API-based access entries rather than the aws-auth ConfigMap. The
    # ConfigMap is the older mechanism and a malformed edit to it locks
    # everyone out of the cluster with no way back in short of recreating it.
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [aws_iam_role_policy_attachment.eks_cluster]

  tags = local.tags
}

resource "aws_iam_role" "eks_nodes" {
  name = "${local.name}-eks-nodes"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "eks_nodes" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])

  role       = aws_iam_role.eks_nodes.name
  policy_arn = each.value
}

resource "aws_eks_node_group" "general" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${local.name}-general"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.private[*].id
  instance_types  = var.eks_node_instance_types

  scaling_config {
    min_size     = var.eks_node_min_size
    max_size     = var.eks_node_max_size
    desired_size = var.eks_node_desired_size
  }

  # One unavailable node at a time during an upgrade. Flink TaskManagers hold
  # state; draining several at once forces repeated job restarts and each
  # restart replays from the last checkpoint.
  update_config {
    max_unavailable = 1
  }

  # desired_size is managed by the cluster autoscaler once the cluster is
  # live. Without this, every terraform apply would fight the autoscaler and
  # reset the node count.
  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.eks_nodes]

  tags = local.tags
}

# --- Workload identity -------------------------------------------------------
#
# IRSA: pods assume an IAM role through a projected service account token
# rather than inheriting the node's instance profile. The difference matters —
# with node-level credentials, every pod on the node can reach the lakehouse
# bucket, including one that only needed to read a config map.

data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]

  tags = local.tags
}

resource "aws_iam_role" "flink_lakehouse" {
  name = "${local.name}-flink-lakehouse"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" =
            "system:serviceaccount:tapeline:flink"
          "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" =
            "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "flink_lakehouse" {
  name = "lakehouse-access"
  role = aws_iam_role.flink_lakehouse.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        # Scoped to this bucket only, not s3:*. The Flink job has no business
        # enumerating every bucket in the account.
        Resource = [aws_s3_bucket.lakehouse.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = ["${aws_s3_bucket.lakehouse.arn}/*"]
      },
    ]
  })
}
