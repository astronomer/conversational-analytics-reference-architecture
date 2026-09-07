{#
  Fails if any gold model reads the wall clock, which would silently return zero
  rows for every recency metric once the calendar moves past the frozen window.
  Comments are stripped before matching, or this rule's own explanation trips it.
#}
{% set offending = [] %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' and 'gold' in node.fqn %}
        {% set lowered = node.raw_code | lower %}
        {% set without_jinja_comments = modules.re.sub('(?s)\{#.*?#\}', ' ', lowered) %}
        {% set code = modules.re.sub('--[^\n]*', ' ', without_jinja_comments) %}
        {% if 'current_date' in code or 'now()' in code %}
            {% do offending.append(node.name) %}
        {% endif %}
    {% endif %}
{% endfor %}

select {{ "'" ~ offending | join(', ') ~ "'" }} as offending_models
where {{ offending | length }} > 0
