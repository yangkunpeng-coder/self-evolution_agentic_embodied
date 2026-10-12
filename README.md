# Self-Evolving Agentic Embodied Framework

> **v1.0.0 · Research source baseline · Fake-only prototype**

一个面向具身智能的 **Event-Sourced + Evidence-Gated Agentic Harness + Offline Self-Evolution** 研究框架。项目关注智能体在任务执行过程中的**权限约束、证据验证、状态恢复、经验积累与离线自进化治理**，为后续仿真实验、模型适配和学术研究提供可扩展的软件基础。

**English summary:** A research-oriented framework for event-sourced, evidence-gated embodied agent execution and offline self-evolution governance. Version 1.0.0 is a **source-code research baseline** with a deterministic Fake Adapter. It does not provide real-robot control, verified autonomous performance improvement, or production-grade safety guarantees.

## 1. 研究定位

本项目主要研究 **Agent Runtime / Harness / Memory / Self-Evolution Governance**，而不是重新训练 VLA、WAM 或底层机器人控制模型。

设计上将执行治理与外部模型、仿真器和设备控制解耦：Harness 负责授权、记录、验证与恢复；Adapter 负责连接具体执行环境。当前仅提供 **Fake Adapter**，用于研究框架内部机制，不涉及真实机器人动作。

核心架构如下：

```text
                         Task / Goal
                             |
                             v
                     Runtime + Harness
             Authorization / Worker Fencing
                             |
                             v
                       Adapter (Fake)
                             |
                     Observation / Feedback
                             |
                             v
                  Evidence + Verification
                             |
                             v
            Event-Sourced SQLite / Recovery
                             |
                             v
                 Experience Collection
                             |
                     Failure Diagnosis
                             |
                             v
                  Evolution Candidates
                             |
                             v
           Offline Evaluation + Human Review
                             |
                             v
                 Release / Rollback Gates
                 (currently fail-closed)
```

## 2. 核心模块

| 模块 | 研究框架中的作用 |
| --- | --- |
| **Runtime / Harness** | 任务生命周期管理、工具授权、执行意图持久化、证据验证与状态提交 |
| **Event Sourcing / Recovery** | 基于 SQLite 的事件记录、哈希链、事务一致性和异常后的保守恢复 |
| **Safety / Concurrency** | Worker Lease、Fencing Token、Stop ACK、执行不确定性隔离及任务版本固定 |
| **Trusted Memory / Skills** | RAW / VERIFIED 记忆分层、Outbox 投影及版本化 Skill Registry |
| **Experience / Diagnosis** | 从执行证据构建经验记录，并对失败现象进行保守分类 |
| **Offline Self-Evolution** | 生成非可执行候选提案，并使用冻结的 Fake 回归场景进行评估 |
| **Review / Governance** | 人工研究审阅、Release Manifest、发布/回滚门禁与审计记录 |

## 3. 仓库结构

GitHub `v1.0.0` 采用**源码精简版**，不包含历史测试工程、故障注入脚本、演示数据库和阶段性开发报告。

```text
self-evolution_agentic_embodied/
├── README.md
├── .gitignore
├── environment.yml
└── framework/
    ├── configs/                    # 框架配置示例
    ├── src/
    │   └── self_evolving_embodied/  # 核心 Python 源码
    └── pyproject.toml              # 包元数据与 CLI 入口
```

## 4. 安装与运行

### 环境要求

- Python **3.12**（包配置约束为 `>=3.12,<3.13`）
- Windows / Miniconda 为当前主要开发环境

首次建立环境时，可在 Windows CMD 执行：

```cmd
conda create -n embodied-research python=3.12 pip -y
conda activate embodied-research
```

如果已经存在 `embodied-research` 环境，直接激活即可。从仓库根目录安装框架：

```cmd
cd framework
python -m pip install -e .
```

### Fake 示例

在 `framework/` 目录中执行：

```cmd
python -m self_evolving_embodied --scenario success --db demo_success.sqlite3
```

或使用安装后的命令行入口：

```cmd
self-evolving-embodied --scenario success --db demo_success.sqlite3
```

## 6. 后续研究方向

本项目将作为持续开发与论文研究的起点，重点探索：

1. **受控仿真与模型适配：** 将 Adapter 接入独立的仿真器或具身模型，建立可重复的任务与观测接口。
2. **证据驱动执行与恢复：** 研究执行证据充分性、状态不确定性和跨任务资源隔离。
3. **自进化效果验证：** 建立可执行候选、对照基线、独立评估集、消融实验与性能指标。
4. **可信治理机制：** 探索身份认证、审批链、版本发布、回滚和设备侧安全边界。
5. **论文可复现性：** 逐步补充实验配置、任务划分、随机种子、统计结果与必要的开放研究材料。

## 7. 版本说明

**`v1.0.0` — Initial Research Source Release**

本版本聚焦核心架构与源码组织。历史开发测试、工具脚本和中间研究资料未随此次精简仓库发布；后续会根据新的研究计划重新建设测试体系、实验基准与论文复现材料。
