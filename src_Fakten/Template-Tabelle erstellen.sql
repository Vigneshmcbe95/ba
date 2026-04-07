-- Parameter
DECLARE @tabname      sysname      = N'tf_lst_abrechn_btr_kug';
DECLARE @themengebiet nvarchar(200) = N'PSD1_DM_STAT_LST_KUG';

-- Zielname sicher bauen
DECLARE @target nvarchar(300) =
    N'dwh.dbo.' + QUOTENAME(@tabname + N'_template');

-------------------------------------------------------------------------------
-- (Optional) vorhandene Template-Tabelle droppen
-------------------------------------------------------------------------------
DECLARE @drop_sql nvarchar(max) =
    N'IF OBJECT_ID(N''' + @target + N''', N''U'') IS NOT NULL DROP TABLE ' + @target + N';';
EXEC (@drop_sql);

-------------------------------------------------------------------------------
-- Dynamisches SELECT INTO mit parametrierten Filtern
-------------------------------------------------------------------------------
DECLARE @sql nvarchar(max) = N'
SELECT DISTINCT
    @p_themengebiet AS themengebiet,
    @p_tabname      AS tabname,
    ddl.colname,
    ddl.colno,

    -- columns_dbo
    CONCAT(
        CHAR(9), LOWER(ddl.COLNAME), '' = '',
        CASE WHEN ddl.IS_NULLABLE = 0 THEN ''ISNULL('' ELSE '''' END,
        CASE 
            WHEN ddl.TYPNAME IN (N''nvarchar'',N''varchar'',N''nchar'',N''char'')
                THEN CONCAT(
                    ''CONVERT('', ddl.TYPNAME COLLATE Latin1_General_100_CI_AS_SC_UTF8,
                    ''('', ddl.COLLENGTH, ''), ''
                )
            ELSE ''''
        END,
        UPPER(ddl.COLNAME),
        CASE 
            WHEN ddl.TYPNAME IN (N''nvarchar'',N''varchar'',N''nchar'',N''char'')
                THEN '' COLLATE Latin1_General_100_CI_AS_SC_UTF8)''
            ELSE ''''
        END,
        CASE
            -- Strings
            WHEN ddl.IS_NULLABLE = 0 AND ddl.TYPNAME LIKE N''%char%''
                THEN '', '''''''')''   -- default: leerer String

            -- Zahlen/Booleans
            WHEN ddl.IS_NULLABLE = 0 AND (
                   ddl.TYPNAME LIKE N''float%''
                OR ddl.TYPNAME IN (N''numeric'',N''decimal'',N''real'',N''money'',N''smallmoney'',N''bit'')
                OR ddl.TYPNAME LIKE N''%int%''
            )
                THEN '', 0)''          -- default: 0

            -- Datums-/Zeit-Typen
            WHEN ddl.IS_NULLABLE = 0 AND (
                   ddl.TYPNAME LIKE N''%date%'' OR ddl.TYPNAME = N''time''
            )
                THEN '', ''''1900-01-01'''')''  -- default: 1900-01-01 (ggf. anpassen für time)

            -- GUID
            WHEN ddl.IS_NULLABLE = 0 AND ddl.TYPNAME = N''uniqueidentifier''
                THEN '', ''''00000000-0000-0000-0000-000000000000'''')''

            -- Binär
            WHEN ddl.IS_NULLABLE = 0 AND ddl.TYPNAME = N''varbinary''
                THEN '', 0x)''          -- default: leeres Binärliteral

            ELSE ''''
        END
    ) AS columns_dbo,

    -- columns_ext
    CONCAT(
        CHAR(9), UPPER(ddl.COLNAME), '' '', ddl.TYPNAME,
        CASE 
            WHEN ddl.TYPNAME IN (N''nvarchar'',N''varchar'',N''nchar'',N''char'')
                THEN CONCAT(''('', ddl.COLLENGTH * 4, '') COLLATE Latin1_General_100_CS_AS_SC_UTF8'')
            WHEN ddl.TYPNAME = N''varbinary''
                THEN CONCAT(''('', ddl.COLLENGTH, '')'')
            WHEN ddl.TYPNAME IN (N''decimal'',N''number'')
                THEN CONCAT(''('', ddl.PRECISION, '','', ddl.SCALE, '')'')
            ELSE ''''
        END,
        '' '',
        CASE WHEN ddl.IS_NULLABLE = 0 THEN ''NOT NULL'' ELSE ''NULL'' END
    ) AS columns_ext

INTO ' + @target + N'
FROM DWH.ext.vm_ddl_sql_server ddl
WHERE CAST(ddl.THMNAME AS nvarchar(50)) COLLATE Latin1_General_100_CI_AS_SC_UTF8 = @p_themengebiet
  AND CAST(ddl.TABNAME AS nvarchar(50)) COLLATE Latin1_General_100_CI_AS_SC_UTF8 = @p_tabname;
';

-- Debug bei Bedarf:
-- PRINT @sql;

EXEC sys.sp_executesql
    @sql,
    N'@p_themengebiet nvarchar(200), @p_tabname nvarchar(200)',
    @p_themengebiet = @themengebiet,
    @p_tabname      = @tabname;

-------------------------------------------------------------------------------
-- RowsInserted zurückgeben
-------------------------------------------------------------------------------
DECLARE @rows int;
DECLARE @cnt_sql nvarchar(max) =
    N'SELECT @r = COUNT(*) FROM ' + @target + N';';
EXEC sys.sp_executesql @cnt_sql, N'@r int OUTPUT', @r = @rows OUTPUT;

SELECT @rows AS RowsInserted;