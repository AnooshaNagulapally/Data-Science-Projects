# Metric 1 - Completeness - Calculate the null value percentage by Incident Zip for Incident closed date column.
With null_count As (
    select
        count(*) as Total,
        `Incident Zip`
    from
        nyc311.service_request
    where
        `Closed Date` is null
    group by
        `Incident Zip`
)
select
    null_count.Total,
    a.`Incident Zip`,
    a.count,
    ROUND(null_count.Total * 100 / a.count, 2) as null_percentage
from
    nyc311.sr_incident_zip_summary a
    join null_count ON null_count.`Incident Zip` = a.`Incident Zip`;

#Metric 2 - Inconsistency - Percentage of SRs by Zip which have no due dates but have a closing date
WITH missing_dates AS (
    SELECT
        Count(*) AS Totals,
        `Incident Zip`
    FROM
        nyc311.service_request
    where
        `Due Date` is null
        AND `Closed Date` is not null
    GROUP BY
        `Incident Zip`
)
SELECT
    b.`Incident Zip`,
    missing_dates.Totals,
    b.Count,
    round(missing_dates.Totals * 100 / count, 2) AS missing_dates_percent
FROM
    nyc311.sr_incident_zip_summary b
    join missing_dates ON missing_dates.`Incident Zip` = b.`Incident Zip`
Having
    missing_dates_percent > 50
order by
    missing_dates_percent Asc;

#Metric 3 - Validity - percentage of wrong dates entered in closing date by complaint type
WITH wrong_dates AS (
    SELECT
        `Complaint Type`,
        Count(*) AS Total_entries
    FROM
        nyc311.service_request
    where
        `Closed Date` < `Created Date`
    group by
        `Complaint Type`
)
Select
    c.`Complaint Type`,
    wrong_dates.`Total_entries`,
    round(wrong_dates.`Total_entries` * 100 / c.Count, 2) AS Percentage_of_wrongEntries
FROM
    nyc311.sr_complaint_type_summary c
    join wrong_dates ON wrong_dates.`Complaint Type` = c.`Complaint Type`
having
    Percentage_of_wrongEntries > 5
order by
    Percentage_of_wrongEntries Asc;

#Metric 4 - Validity - zip codes being out of range from what is shown in the reference tables.
WITH Zip_code AS (
    SELECT
        cast(
            REGEXP_REPLACE(`Incident Zip`, '[^[:alnum:]]+', '') AS UNSIGNED
        ) newZip
    FROM
        nyc311.service_request
)
SELECT
    SUM(
        CASE
            WHEN `newZip` < 10001 THEN 1
            ELSE 0
        END
    ) below_ZipCode_range,
    SUM(
        CASE
            WHEN `newZip` BETWEEN 10001
            AND 11697 THEN 1
            ELSE 0
        END
    ) valid_ZipCode,
    SUM(
        CASE
            WHEN `newZip` > 11697 THEN 1
            ELSE 0
        END
    ) above_ZipCode_range
FROM
    Zip_code;

#Metric 5 - Accuracy - percentage of Zip codes are present in data which are outside the NYC region.
WITH TotalVal AS (
    SELECT
        SUM(wrong_zipCount) x
    From
        (
            select
                `Incident Zip`,
                COUNT(*) AS wrong_zipCount
            from
                nyc311.service_request
            where
                `Incident Zip` NOT IN (
                    select
                        `Zip`
                    from
                        nyc311.zip_code_nyc_borough
                )
            group by
                `Incident Zip`
        ) a
)
SELECT
    ROUND(
        x * 100 /(
            SELECT
                COUNT(*)
            FROM
                nyc311.service_request
        ),
        2
    ) AS percentage_wrong_zip
FROM
    TotalVal;

#Metric 6 - Accuracy - Wrong Borough names in the SR data against to respective Zip codes.
WITH Wrong_boroughs AS (
    SELECT
        `Incident Zip`,
        lower(a.`Borough`) AS sr_borough,
        b.`Zip`,
        lower(b.`Borough`) AS ref_borough
    FROM
        nyc311.service_request a
        LEFT JOIN nyc311.zip_code_nyc_borough b ON a.`Incident Zip` = b.`Zip`
    WHERE
        `Incident Zip` = `Zip`
        AND a.`Borough` <> b.`Borough`
    group by
        `Incident Zip`,
        a.`Borough`
)
SELECT
    ROUND(
        COUNT(*) * 100 /(
            SELECT
                COUNT(*)
            FROM
                (
                    SELECT
                        `Incident Zip`,
                        `Borough`
                    FROM
                        nyc311.service_request
                    group by
                        `Incident Zip`,
                        `Borough`
                ) g
        ),
        2
    ) AS percent_wrongBoroughs
From
    Wrong_boroughs;

#Metric 7 - Accuracy - Percentage of wrong complaint types present in SR data.
WITH TotalTypes AS (
    SELECT
        SUM(wrong_ComplaintType) z
    From
        (
            select
                `Complaint Type`,
                COUNT(*) AS wrong_ComplaintType
            from
                nyc311.service_request
            where
                `Complaint Type` NOT IN (
                    select
                        `Type`
                    from
                        nyc311.ref_sr_type_nyc311_open_data_26
                )
            group by
                `Complaint Type`
        ) a
)
SELECT
    ROUND(
        z * 100 /(
            SELECT
                COUNT(*)
            FROM
                nyc311.service_request
        )
    ) AS percentage_wrong_complaintType
FROM
    TotalTypes;

#Metric 8- Completeness - Calculate the total percentage of rows that do not have null values at least in one of the 
# chosen columns (Created Date, Closed Date, Due Date, Borough, Complaint Type, Incident Zip)
WITH Complete_Data AS (
    SELECT
        COUNT(*) AS T
    FROM
        nyc311.service_request
    where
        `Incident Zip` Is Not Null
        And `Created Date` Is Not Null
        And `Closed Date` Is Not Null
        And `Complaint Type` Is Not Null
        And `Due Date` Is Not Null
        And `Borough` Is Not Null
)
SELECT
    ROUND(
        T * 100 /(
            SELECT
                COUNT(*)
            FROM
                nyc311.service_request
        )
    ) AS CompleteData_Percentage
FROM
    Complete_Data;