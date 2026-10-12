# Self-Evolving Agentic Embodied Framework

**Version: 1.1.0rc3 · Python 3.12 · 通用框架发布候选版**

一个 Agent，以 LLM 为唯一 Brain；执行工具箱、独立评估工具箱、实时内存 Observation Pipeline、受授权 Harness 与离线自进化治理协作。核心运行只依赖 Python 标准库，ROS 2、设备、仿真器与模型 SDK 均通过可选接口接入。

```cmd
conda activate embodied-research
cd /d D:\work\python_project\self-evolution_agentic_embodied\framework
python -m pip install -e ".[dev]"
python -m pytest -q
python examples\framework_minimal.py
python examples\plugin_registration.py
python examples\observation_pipeline.py
```

三个示例分别演示统一初始化、可信插件注册/TOML 装配和四种观测读取。使用本地 Stub LLM、只读工具和合成传感器数据，无 API Key 或机器人依赖。

真实宿主实现 `TextCompletionProvider.complete(system_prompt, user_prompt)`，通过 `FrameworkBuilder(..., provider=provider)` 注入。Execution Tool 工厂接收限定范围的 `ObservationReader`；Assessment Tool 返回 `SUPPORTED / NOT_SUPPORTED / UNKNOWN`。注册物理动作工具不会自动获得权限，必须另行通过宿主 `ExecutionGrant` 和既有 Harness 接口授权。

SQLite 保存任务、事件链、评估摘要和版本化策略，原始传感器流保留在独立有界内存队列中；默认 60 秒，容量默认 `ceil(frequency_hz × retention_seconds)`，可用每流 `capacity` 覆盖。全局内存预算可能缩短实际保留窗口。

- [完整初始化与评估反馈](examples/framework_minimal.py)
- [可信插件注册与 TOML 配置](examples/plugin_registration.py)
- [Observation Pipeline 接口](examples/observation_pipeline.py)

可选安装 `python -m pip install -e ".[llm]"` 提供 OpenAI-compatible Provider SDK。ROS 2 的 `rclpy` 由 ROS 宿主管理，不作为普通 pip 强制依赖。

包版本保持 RC3。合成验收不证明真实机器人安全、模型任务效果或自进化性能收益。正式版发布需维护者确认。
