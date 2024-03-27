{% macro create_row_access_policy_rap_encrypt(node_database,node_schema) %}

    CREATE ROW ACCESS POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.rap_encrypt AS (val varchar)

    RETURNS BOOLEAN ->
        CASE WHEN CURRENT_ROLE() IN ('ANALYST','SYSADMIN') THEN TRUE
        ELSE FALSE
        END

{% endmacro %}
