{% macro apply_row_access_policy_list_for_sources(meta_key,operation_type="apply") %}

{% if execute %}

    {% for node in graph.sources.values() -%}

        {% set database = node.database | string %}
        {% set schema   = node.schema | string %}
        {% set name   = node.name | string %}
        {% set identifier = (node.identifier | default(name, True)) | string %}

        {% set unique_id = node.unique_id | string %}
        {% set resource_type = node.resource_type | string %}
        {% set materialization = "table" %}

        {% set relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
        {% if relation.is_view %}
          {% set materialization = "view" %}
        {% endif %}

        {% set meta_columns = get_meta_objects(unique_id,meta_key,resource_type) %}

        {# Use the database and schema for the source node: #}
        {#     In the apple for models variant of this file it instead uses the model.database/schema metadata #}
        {% set row_access_policy_db = node.database %}
        {% set row_access_policy_schema = node.schema %}

        {# Override the database and schema name when use_common_row_access_policy_db flag is set #}
        {%- if (var('use_common_row_access_policy_db', 'False')|upper in ['TRUE','YES']) -%}
            {% if (var('common_row_access_policy_db') and var('common_row_access_policy_schema')) %}
                {% set row_access_policy_db = var('common_row_access_policy_db') | string  %}
                {% set row_access_policy_schema = var('common_row_access_policy_schema') | string  %}
            {% endif %}
        {% endif %}

        {# Override the schema name (in the row_access_policy_db) when use_common_row_access_policy_schema_only flag is set #}
        {%- if (var('use_common_row_access_policy_schema_only', 'False')|upper in ['TRUE','YES']) and (var('use_common_row_access_policy_db', 'False')|upper in ['FALSE','NO']) -%}
            {% if var('common_row_access_policy_schema') %}
                {% set row_access_policy_schema = var('common_row_access_policy_schema') | string  %}
            {% endif %}
        {% endif %}

        {% set row_access_policy_list_sql %}
            show row access policies in {{row_access_policy_db}}.{{row_access_policy_schema}};
            select $3||'.'||$4||'.'||$2 as row_access_policy from table(result_scan(last_query_id()));
        {% endset %}

        {# If there are some row_access policies to be applied in this model, we should show the row_access policies in the schema #}
        {% if meta_columns | length > 0 %}
            {% set row_access_policy_list = dbt_utils.get_query_results_as_dict(row_access_policy_list_sql) %}
        {% endif %}

        {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
            {% set column               = meta_tuple[0] %}
            {% set row_access_policy_name  = meta_tuple[1] %}
            {% set conditional_columns  = meta_tuple[2] %}

            {% if row_access_policy_name is not none %}

                {% for row_access_policy_in_db in row_access_policy_list['ROW_ACCESS_POLICY'] %}
                    {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper == row_access_policy_in_db %}
                        {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row_access policy to source : " ~ row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ identifier ~ '.' ~ column, info=True) }}
                        {% set query %}
                            {% if operation_type == "apply" %}
                                alter {{materialization}} {{database}}.{{schema}}.{{identifier}}
                                ADD row access policy {{row_access_policy_db}}.{{row_access_policy_schema}}.{{row_access_policy_name}} ON ({{column}}) {% if conditional_columns | length > 0 %}using ({{column}}, {{conditional_columns|join(', ')}}){% endif %};
                            {% elif operation_type == "unapply" %}
                                alter {{materialization}}  {{database}}.{{schema}}.{{identifier}} drop row access policy  {{row_access_policy_name}}
                            {% endif %}
                        {% endset %}
                        {% do run_query(query) %}
                    {% endif %}
                {% endfor %}
            {% endif %}

        {% endfor %}

    {% endfor %}

{% endif %}

{% endmacro %}
