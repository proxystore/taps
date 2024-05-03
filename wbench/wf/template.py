from __future__ import annotations

from wbench.workflow import register


@register(name='template')
class TemplateWorkflow:
    name = 'template'
