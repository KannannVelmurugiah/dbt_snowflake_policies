{% macro unapply_row_access_policy(resource_type="models",meta_key="row_access_policy",operation_type="unapply") %}

    {% if execute %}

        {% if resource_type == "sources" %}
            {{ apply_row_access_policy_list_for_sources(meta_key,operation_type) }}
        {% elif resource_type|lower in ["models", "snapshots"] %}
            {{ apply_row_access_policy_list_for_models(meta_key,operation_type) }}
        {% endif %}

    {% endif %}

{% endmacro %}
