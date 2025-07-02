USE msdb;
GO

-- STEP 1: Extract cleanup time per job
;WITH CleanupTimes AS (
    SELECT 
        CASE 
            WHEN sj.name LIKE '%FULL%' AND sj.name LIKE '%SYSTEM%' THEN 'SystemFull'
            WHEN sj.name LIKE '%FULL%' THEN 'UserFull'
            WHEN sj.name LIKE '%DIFF%' THEN 'UserDiff'
            WHEN sj.name LIKE '%LOG%' THEN 'UserLog'
            ELSE 'Other'
        END AS BackupCategory,
        TRY_CAST(
            REPLACE(REPLACE(
                LTRIM(RTRIM(
                    SUBSTRING(
                        sjs.command,
                        CHARINDEX('@CleanupTime', sjs.command) + LEN('@CleanupTime ='),
                        CHARINDEX(',', sjs.command + ',', CHARINDEX('@CleanupTime', sjs.command)) 
                            - (CHARINDEX('@CleanupTime', sjs.command) + LEN('@CleanupTime ='))
                    )
                )),
                '''', ''
            ), '"', '') AS INT
        ) AS CleanupHours
    FROM msdb.dbo.sysjobs sj
    INNER JOIN msdb.dbo.sysjobsteps sjs ON sj.job_id = sjs.job_id
    WHERE sjs.command LIKE '%DatabaseBackup%' AND sjs.command LIKE '%@CleanupTime%'
),

-- STEP 2: Backup records with consistent categories
BackupHistory AS (
    SELECT
        b.database_name,
        b.backup_start_date,
        b.backup_finish_date,
        b.backup_size,
        b.type,
        b.recovery_model,
        mf.physical_device_name,
        CASE 
            WHEN b.type = 'D' AND LOWER(b.database_name) IN ('master', 'model', 'msdb') THEN 'SystemFull'
            WHEN b.type = 'D' THEN 'UserFull'
            WHEN b.type = 'I' THEN 'UserDiff'
            WHEN b.type = 'L' THEN 'UserLog'
            ELSE 'Other'
        END AS BackupCategory
    FROM msdb.dbo.backupset b
    INNER JOIN msdb.dbo.backupmediafamily mf ON b.media_set_id = mf.media_set_id
),

-- STEP 3: One latest backup per database + type
LatestBackup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY database_name, BackupCategory
                   ORDER BY backup_start_date DESC
               ) AS rn
        FROM BackupHistory
    ) x
    WHERE x.rn = 1
)

-- STEP 4: Join CleanupTime + Final Report
SELECT
    SERVERPROPERTY('MachineName') AS ServerName,
    lb.database_name,
    CASE lb.type
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Log'
        ELSE lb.type
    END AS BackupType,
    lb.backup_start_date,
    lb.backup_finish_date,
    DATEDIFF(MINUTE, lb.backup_start_date, lb.backup_finish_date) AS DurationMinutes,
    CAST(lb.backup_size / 1048576.0 AS DECIMAL(18,2)) AS BackupSizeMB,
    lb.physical_device_name AS BackupFile,
    lb.recovery_model,
    ct.CleanupHours AS RetentionHours,
    CASE 
        WHEN ct.CleanupHours IS NULL THEN 'Unknown (No CleanupTime)'
        WHEN lb.backup_start_date < DATEADD(HOUR, -ct.CleanupHours, GETDATE()) THEN 'Expired'
        ELSE 'Valid'
    END AS RetentionStatus
FROM LatestBackup lb
LEFT JOIN CleanupTimes ct ON lb.BackupCategory = ct.BackupCategory
ORDER BY lb.database_name, BackupType;
