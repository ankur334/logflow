from transformer.base_transformer import AbstractTransformer


class FlinkLogPromoteTransform(AbstractTransformer):
    def transform(self, record):  # not used in Flink path
        return record

    def apply_in_flink(self, t_env, source_table: str) -> str:
        view = "logs_enriched"
        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW `{view}` AS
            SELECT
                ts,
                serviceName,
                severityText,
                attributes['msg'] AS msg,
                attributes['url'] AS url,
                COALESCE(CAST(JSON_VALUE(body, '$.data.mobile') AS STRING), attributes['mobile']) AS mobile,
                attributes,
                resources,
                body,
                DATE_FORMAT(ts, 'yyyy-MM-dd') AS dt,
                DATE_FORMAT(ts, 'HH')         AS hr
            FROM `{source_table}`
        """)
        return view
