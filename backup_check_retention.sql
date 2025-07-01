USE msdb;
GO

-- Step 1: Get CleanupTime per Job and map it to BackupType
;WITH CleanupTimes AS (
    SELECT 
        sj.name AS JobName,
        CASE 
            WHEN sj.name LIKE '%FULL%' AND sj.name LIKE '%SYSTEM%' THEN 'SystemFull'
            WHEN sj.name LIKE '%FULL%' THEN 'UserFull'
            WHEN sj.name LIKE '%DIFF%' THEN 'UserDiff'
            WHEN sj.name LIKE '%LOG%' THEN 'UserLog'
            ELSE 'Other'
        END AS BackupCategory,
        TRY_CAST(
            REPLACE(
                REPLACE(
                    LTRIM(RTRIM(
                        SUBSTRING(
                            sjs.command,
                            CHARINDEX('@CleanupTime', sjs.command) + LEN('@CleanupTime ='),
                            CHARINDEX(',', sjs.command + ',', CHARINDEX('@CleanupTime', sjs.command)) 
                                - (CHARINDEX('@CleanupTime', sjs.command) + LEN('@CleanupTime ='))
                        )
                    )),
                    '''', ''  -- remove single quotes
                ),
                '"', ''      -- remove double quotes
            ) AS INT
        ) AS CleanupHours
    FROM msdb.dbo.sysjobs sj
    INNER JOIN msdb.dbo.sysjobsteps sjs ON sj.job_id = sjs.job_id
    WHERE sjs.command LIKE '%DatabaseBackup%'
      AND sjs.command LIKE '%@CleanupTime%'
)

-- Step 2: Backup history + Match backup type to CleanupTimes
SELECT
    SERVERPROPERTY('MachineName') AS ServerName,
    b.database_name,
    CASE b.type
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Log'
        ELSE b.type
    END AS BackupType,
    b.backup_start_date,
    b.backup_finish_date,
    DATEDIFF(MINUTE, b.backup_start_date, b.backup_finish_date) AS DurationMinutes,
    CAST(b.backup_size / 1048576.0 AS DECIMAL(18,2)) AS BackupSizeMB,
    mf.physical_device_name AS BackupFile,
    b.recovery_model,
    
    -- Determine backup category to join with CleanupTime
    CASE 
        WHEN b.type = 'D' AND b.database_name IN ('master', 'model', 'msdb') THEN 'SystemFull'
        WHEN b.type = 'D' THEN 'UserFull'
        WHEN b.type = 'I' THEN 'UserDiff'
        WHEN b.type = 'L' THEN 'UserLog'
        ELSE 'Other'
    END AS BackupCategory,

    ct.CleanupHours,
    CASE 
        WHEN ct.CleanupHours IS NULL THEN 'Unknown (No CleanupTime)'
        WHEN b.backup_start_date < DATEADD(HOUR, -ct.CleanupHours, GETDATE()) THEN 'Expired'
        ELSE 'Valid'
    END AS RetentionStatus

FROM msdb.dbo.backupset b
INNER JOIN msdb.dbo.backupmediafamily mf ON b.media_set_id = mf.media_set_id
LEFT JOIN CleanupTimes ct 
    ON (
        (b.type = 'D' AND b.database_name IN ('master', 'model', 'msdb') AND ct.BackupCategory = 'SystemFull')
        OR (b.type = 'D' AND b.database_name NOT IN ('master', 'model', 'msdb') AND ct.BackupCategory = 'UserFull')
        OR (b.type = 'I' AND ct.BackupCategory = 'UserDiff')
        OR (b.type = 'L' AND ct.BackupCategory = 'UserLog')
    )
WHERE b.backup_start_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY b.database_name, b.backup_start_date DESC;
