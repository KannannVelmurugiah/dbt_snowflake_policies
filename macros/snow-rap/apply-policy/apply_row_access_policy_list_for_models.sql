{% macro apply_row_access_policy_list_for_models(meta_key,operation_type="apply") %}

{% if execute %}

    {% if operation_type == "apply" %}

        {% set model_id = model.unique_id | string %}
        {% set alias    = model.alias %}
        {% set database = model.database %}
        {% set schema   = model.schema %}
        {% set model_resource_type = model.resource_type | string %}

        {% if model_resource_type|lower in ["model", "snapshot"] %}

            {# This dictionary stores a mapping between materializations in dbt and the objects they will generate in Snowflake  #}
            {% set materialization_map = {"table": "table", "view": "view", "incremental": "table", "snapshot": "table", "dynamic_table": "table"} %}

            {# Append custom materializations to the list of standard materializations  #}
            {% do materialization_map.update(fromjson(var('custom_materializations_map', '{}'))) %}

            {% set materialization = materialization_map[model.config.get("materialized")] %}
            {% set meta_columns = get_meta_objects(model_id,meta_key) %}

            {% set row_access_policy_db = model.database %}
            {% set row_access_policy_schema = model.schema %}

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

            {# If there are some row access policies to be applied in this model, we should show the row access policies in the schema #}
            {% if meta_columns | length > 0 %}
                {% set row_access_policy_list = dbt_utils.get_query_results_as_dict(row_access_policy_list_sql) %}
            {% endif %}

            {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
                {% set column               = meta_tuple[0] %}
                {% set row_access_policy_name  = meta_tuple[1] %}
                {% set conditional_columns  = meta_tuple[2] %}

                {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper not in row_access_policy_list['ROW_ACCESS_POLICY'] %}
                    {{ exceptions.raise_compiler_error("Row access policy "~ row_access_policy_name ~" does not exist in the database. Kindly make sure to create the policies before you apply it.") }}
                {% endif %}

                {% if row_access_policy_name is not none %}

                    {% for row_access_policy_in_db in row_access_policy_list['ROW_ACCESS_POLICY'] %}

                        {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper == row_access_policy_in_db %}
                            {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row_access policy to model  : " ~ row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ alias ~ '.' ~ column, info=True) }}
                            {% set query %}
                            alter {{materialization}} {{database}}.{{schema}}.{{alias}}
                            ADD row access policy {{row_access_policy_db}}.{{row_access_policy_schema}}.{{row_access_policy_name}} ON ({{column}}) {% if conditional_columns | length > 0 %}using ({{column}}, {{conditional_columns|join(', ')}}){% endif %} ;
                            {% endset %}
                            {% do run_query(query) %}
                            {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ "creating clone of the model with suffix _clone", info=True) }}
                            {% if materialization =="table" %}
                                {% set query %}
                                create or replace {{materialization}} {{database}}.{{schema}}.{{alias}}_clone CLONE {{database}}.{{schema}}.{{alias}} ;
                                {% endset %}
                                {% do run_query(query) %}
                            {% elif materialization == "view" %}
                                {% set query %}
                                create or replace {{materialization}} {{database}}.{{schema}}.{{alias}}_clone as select * from {{database}}.{{schema}}.{{alias}} ;
                                {% endset %}
                                {% do run_query(query) %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}

                {% else %}
                    {% set query %}
                    select 1 ;
                    {% endset %}
                    {% do run_query(query) %}
                    {{ exceptions.raise_compiler_error("ROW ACCESS POLICY DOES NOT EXIST") }}
                {% endif %}
            {% endfor %}

        {% endif %}

    {% elif operation_type == "unapply" %}

        {% for node in graph.nodes.values() -%}

            {% set database = node.database | string %}
            {% set schema   = node.schema | string %}
            {% set node_unique_id = node.unique_id | string %}
            {% set node_resource_type = node.resource_type | string %}
            {% set materialization_map = {"table": "table", "view": "view", "incremental": "table", "snapshot": "table", "dynamic_table": "table"} %}

            {% if node_resource_type|lower in ["model", "snapshot"] %}

                {# Append custom materializations to the list of standard materializations  #}
                {% do materialization_map.update(fromjson(var('custom_materializations_map', '{}'))) %}

                {% set materialization = materialization_map[node.config.get("materialized")] %}
                {% set alias    = node.alias %}

                {% set meta_columns = get_meta_objects(node_unique_id,meta_key,node_resource_type) %}

                {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
                    {% set column   = meta_tuple[0] %}
                    {% set row_access_policy_name  = meta_tuple[1] %}

                    {% if row_access_policy_name is not none %}
                        {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row_access policy to model  : " ~ database|upper ~ '.' ~ schema|upper ~ '.' ~ row_access_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ alias ~ '.' ~ column, info=True) }}
                        {% set query %}
                            alter {{materialization}}  {{database}}.{{schema}}.{{alias}} drop row access policy  {{row_access_policy_name}}
                        {% endset %}
                        {% do run_query(query) %}
                    {% endif %}

                {% endfor %}

            {% endif %}

        {% endfor %}

    {% endif %}

{% endif %}

{% endmacro %}
