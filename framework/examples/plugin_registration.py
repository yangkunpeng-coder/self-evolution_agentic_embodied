"""Trusted plugin registration + TOML selection + actual read-only Agent task."""
from pathlib import Path
from tempfile import TemporaryDirectory

from self_evolving_embodied import (
    ObservationInput, PluginKind, PluginManifest, TrustedPluginCatalog,
    build_configured_framework, load_framework_settings,
)
from framework_minimal import (
    ASSESSOR, KEY, NEEDS, ROUTE, TOOL, MetadataTool, StubLLM, assess_factory, report_policy,
)


def main() -> None:
    catalog = TrustedPluginCatalog()
    catalog.register_llm_provider(PluginManifest('host.llm', '1.0.0', PluginKind.LLM_PROVIDER, 'Local Stub LLM'), StubLLM)
    catalog.register_execution(PluginManifest('host.inspect', TOOL.version, PluginKind.EXECUTION, 'Metadata tool'),
                               TOOL, needs=NEEDS, factory=MetadataTool)
    catalog.register_assessor(PluginManifest('host.assessor', ASSESSOR.version, PluginKind.ASSESSMENT, 'Rule assessor'),
                              ASSESSOR, factory=assess_factory)
    with TemporaryDirectory(prefix='embodied-plugins-') as folder:
        config_path = Path(folder)/'framework.toml'
        config_path.write_text('''[framework]
database_path = "tasks.sqlite3"
[[streams]]
source_id = "synthetic_sensor"
modality = "metadata"
frequency_hz = 30
capacity = 1800
[plugins]
brain = {id = "host.llm", version = "1.0.0"}
execution = [{id = "host.inspect", version = "1.0.0"}]
assessment = [{id = "host.assessor", version = "1.0.0"}]
''', encoding='utf-8')
        # Evidence policies/routes come from trusted Python host code.
        app = build_configured_framework(load_framework_settings(config_path), catalog,
            evidence_policies={'report_available': report_policy}, assessment_routes=(ROUTE,))
        with app:
            app.publish_observation(ObservationInput(KEY, {'visible': True}))
            report = app.run_instruction('Inspect synthetic object')
            assert report.status == 'EXECUTION_WORKFLOW_COMPLETED' and report.last_outcome == 'SUPPORTED'
            print(report)


if __name__ == '__main__':
    main()
