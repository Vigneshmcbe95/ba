-- ============================================================
-- SKRIPT: Externe Tabellen pro Partition erstellen (ext.)
-- Format: ext.tf_lst_abrechn_btr_kug_<HIGH_VALUE>
-- ============================================================
DECLARE @t          nvarchar(128) = N'TF_LST_ABRECHN_BTR_KUG';
DECLARE @t_template nvarchar(128) = N'TF_LST_ABRECHN_BTR_KUG_TEMPLATE';
DECLARE @d          nvarchar(128) = N'ESTAT';
DECLARE @s          nvarchar(128) = N'PSD1_DM_STAT_LST_KUG';

-- Partitionsliste laden (HIGH_VALUE fuer Praedikat-Pushdown)
IF OBJECT_ID('tempdb..#partitions') IS NOT NULL DROP TABLE #partitions;
SELECT HIGH_VALUE
INTO #partitions
FROM [DWH].[ext].[v_all_tab_partitions]
WHERE TABLE_NAME  = 'TF_LST_ABRECHN_BTR_KUG'
  AND TABLE_OWNER = 'PSD1_DM_STAT_LST_KUG';

-- Schleifenvariablen
DECLARE @high_value     nvarchar(256);
DECLARE @ext_table_name nvarchar(256);
DECLARE @drop           nvarchar(max);
DECLARE @crt            nvarchar(max);
DECLARE @sql_cols       nvarchar(max);
DECLARE @cols           nvarchar(max);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT HIGH_VALUE
    FROM #partitions;

OPEN cur;
FETCH NEXT FROM cur INTO @high_value;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Tabellenname zusammensetzen
    SET @ext_table_name = LOWER(@t) + '_' + @high_value;

    -- Tabelle loeschen falls bereits vorhanden
    SET @drop = CONCAT(
        N'IF EXISTS (SELECT 1 FROM sys.external_tables ',
        N'           WHERE schema_id = SCHEMA_ID(''ext'') ',
        N'             AND name = ''', @ext_table_name, ''') ',
        N'DROP EXTERNAL TABLE ext.', @ext_table_name, ';'
    );

    -- Spaltenliste aus Vorlagetabelle lesen
    SET @cols = NULL;
    SET @sql_cols = CONCAT(
        N'SELECT @cols = STRING_AGG(CAST(columns_ext AS nvarchar(max)), '','' + CHAR(13)+CHAR(10)) ',
        N'WITHIN GROUP (ORDER BY colno) ',
        N'FROM dwh.dbo.', @t_template, ' ',
        N'WHERE tabname      = ''', @t, ''' ',
        N'  AND themengebiet = ''', @s, ''';'
    );
    EXEC sp_executesql
        @sql_cols,
        N'@cols nvarchar(max) OUTPUT',
        @cols = @cols OUTPUT;

    -- Externe Tabelle erstellen
    -- Speicherort: ESTAT.PSD1_DM_STAT_LST_KUG.TF_LST_ABRECHN_BTR_KUG
    SET @crt = CONCAT(
        N'CREATE EXTERNAL TABLE ext.', @ext_table_name, N' (', CHAR(13)+CHAR(10),
        @cols, CHAR(13)+CHAR(10),
        N') WITH (DATA_SOURCE=[cman-idst], LOCATION=''',
        @d, '.', @s, '.', @t,
        ''');'
    );

    -- Ausfuehren: loeschen und neu erstellen
    EXEC(@drop);
    EXEC(@crt);
    PRINT 'Externe Tabelle erstellt: ext.' + @ext_table_name;

    FETCH NEXT FROM cur INTO @high_value;
END;

CLOSE cur;
DEALLOCATE cur;
DROP TABLE #partitions;

PRINT 'Fertig - alle externen Partitionstabellen wurden erfolgreich erstellt.';