{% macro bucket_numeric(expr, edges, labels=none) %}
  {# edges: array of numeric upper-bounds, e.g. [2, 2.5, 3, 3.5, 4, 99] #}
  case
  {% for i in range(edges|length) %}
    when {{ expr }} <= {{ edges[i] }} then {{ "'" ~ (labels[i] if labels else ("<= " ~ edges[i]|string)) ~ "'" }}
  {% endfor %}
  else 'other'
  end
{% endmacro %}

{% macro bucket_boolean(expr) %}
  case when {{ expr }} then 'true' else 'false' end
{% endmacro %}
