SELECT
    phrase,
    arrayReverse(
        arrayFilter(
            x -> x.2 > 0,
            arrayMap(
                (hour_val, diff_val) -> (hour_val, diff_val),
                hours,
                arrayDifference(views_array)
            )
        )
    ) AS views_by_hour
FROM
    (
        SELECT
            phrase,
            groupArray(h) AS hours,
            groupArray(max_v) AS views_array
        FROM
            (
                SELECT
                    phrase,
                    toHour(dt) AS h,
                    max(views) AS max_v
                FROM
                    phrases_views
                WHERE
                    campaign_id = 1111111
                    AND toDate(dt) = today()
                GROUP BY
                    phrase,
                    h
                ORDER BY
                    h ASC
            )
        GROUP BY
            phrase
    );