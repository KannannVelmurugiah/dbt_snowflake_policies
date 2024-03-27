{% macro apply_row_access_policy(resource_type="models",meta_key="row_access_policy") %}

    {% if execute %}

        {% if resource_type == "sources" %}
            {{ apply_row_access_policy_list_for_sources(meta_key) }}
        {% elif resource_type|lower in ["models", "snapshots"] %}
            {{ apply_row_access_policy_list_for_models(meta_key) }}
        {% endif %}

    {% endif %}

{% endmacro %}
