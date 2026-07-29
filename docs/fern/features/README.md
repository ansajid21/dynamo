---
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
title: Feature Guides
subtitle: Start with Dynamo's core serving optimizations, then branch into operations and model capabilities.
---

Use these guides after you have Dynamo running and want to improve serving behavior, operate a deployment, or adapt Dynamo to a new workload.

## Recommended path

Most deployments start with the core performance loop:

| Step | Guide | Use when |
|---|---|---|
| 1 | [KV Cache Aware Routing](../developer-guide/knowledge-base/modular-components/router/router-guide.md) | Route requests to workers that already hold useful KV cache. |
| 2 | [Disaggregated Serving](../kubernetes/model-deployment/disaggregated-serving.md) | Scale prefill and decode workers independently. |
| 3 | [KV Cache Offloading](../developer-guide/knowledge-base/modular-components/kvbm/kvbm-guide.md) | Extend usable cache capacity beyond GPU memory. |
| 4 | [Benchmarking](../recipes/feature-benchmarks/benchmarking-guide.md) | Compare configurations before you move to production. |

## Where to go next

| Goal | Start with |
|---|---|
| Make serving more resilient | [Fault Tolerance](../kubernetes/fault-tolerance/introduction.md) |
| Monitor local deployments | [Install Observability](../cli/installation/observability.mdx) |
| Reproduce traffic without a full engine | [Live Simulation with Mocker](../kubernetes/operations/dynosim/mocker-live-simulation.mdx) |
| Add structured model outputs | [Tool Calling](tool-calling-and-reasoning/tool-call-parsing.mdx) and [Reasoning](tool-calling-and-reasoning/reasoning-parsing.md) |
| Build agent workloads | [Agents](agents/overview.md) |
| Serve specialized workloads | [LoRA Adapters](lora-adapters/overview.md), [Multimodal](multimodal-serving/overview.md), and [Diffusion](diffusion/overview.md) |

For cluster deployments, pair these guides with the [Kubernetes Deployment](../kubernetes/getting-started/quickstart.mdx) docs. The same features can be explored locally, then expressed through Dynamo's Kubernetes-native CRDs and operator when you move to a shared GPU cluster.
