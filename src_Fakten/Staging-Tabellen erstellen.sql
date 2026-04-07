-- ============================================================
-- SKRIPT: Erstelle dbo._in + dbo._out pro Partition
-- ============================================================
DECLARE @t          nvarchar(128) = N'TF_LST_ABRECHN_BTR_KUG';
DECLARE @t_template nvarchar(128) = N'TF_LST_ABRECHN_BTR_KUG_TEMPLATE';
DECLARE @s          nvarchar(128) = N'PSD1_DM_STAT_LST_KUG';

IF OBJECT_ID('tempdb..#partitions') IS NOT NULL DROP TABLE #partitions;

SELECT HIGH_VALUE
INTO #partitions
FROM [DWH].[ext].[v_all_tab_partitions]
WHERE TABLE_NAME  = 'TF_LST_ABRECHN_BTR_KUG'
  AND TABLE_OWNER = 'PSD1_DM_STAT_LST_KUG';

-- Schleifenvariablen
DECLARE @high_value    nvarchar(256);
DECLARE @dbo_table_in  nvarchar(256);
DECLARE @dbo_table_out nvarchar(256);
DECLARE @drop_in       nvarchar(max);
DECLARE @drop_out      nvarchar(max);
DECLARE @crt_in        nvarchar(max);
DECLARE @crt_out       nvarchar(max);
DECLARE @sql_cols      nvarchar(max);
DECLARE @cols          nvarchar(max);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT HIGH_VALUE FROM #partitions;

OPEN cur;
FETCH NEXT FROM cur INTO @high_value;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Tabellennamen zusammensetzen
    SET @dbo_table_in  = LOWER(@t) + '_in_'  + @high_value;
    SET @dbo_table_out = LOWER(@t) + '_out_' + @high_value;

    -- ── Spaltenliste aus Vorlagetabelle lesen ──────────────
    SET @cols = NULL;
    SET @sql_cols = CONCAT(
        N'SELECT @cols = STRING_AGG(CAST(columns_ext AS nvarchar(max)), '','' + CHAR(13)+CHAR(10)) ',
        N'WITHIN GROUP (ORDER BY colno) ',
        N'FROM dwh.dbo.', @t_template, ' ',
        N'WHERE tabname      = ''', @t, ''' ',
        N'  AND themengebiet = ''', @s, ''';'
    );
    EXEC sp_executesql @sql_cols, N'@cols nvarchar(max) OUTPUT', @cols = @cols OUTPUT;

    -- ── Eingangs-Tabelle loeschen falls vorhanden ──────────
    SET @drop_in = CONCAT(
        N'IF EXISTS (SELECT 1 FROM sys.tables ',
        N'           WHERE schema_id = SCHEMA_ID(''dbo'') ',
        N'             AND name = ''', @dbo_table_in, ''') ',
        N'DROP TABLE dbo.[', @dbo_table_in, '];'
    );

    -- ── Ausgangs-Tabelle loeschen falls vorhanden ──────────
    SET @drop_out = CONCAT(
        N'IF EXISTS (SELECT 1 FROM sys.tables ',
        N'           WHERE schema_id = SCHEMA_ID(''dbo'') ',
        N'             AND name = ''', @dbo_table_out, ''') ',
        N'DROP TABLE dbo.[', @dbo_table_out, '];'
    );

    -- ── Eingangs-Tabelle erstellen (Quelldaten) ────────────
    SET @crt_in = CONCAT(
        N'CREATE TABLE dbo.[', @dbo_table_in, N'] (', CHAR(13)+CHAR(10),
        @cols, CHAR(13)+CHAR(10),
        N');'
    );

    -- ── Ausgangs-Tabelle erstellen (Staging/Ziel) ──────────
    SET @crt_out = CONCAT(
        N'CREATE TABLE dbo.[', @dbo_table_out, N'] (', CHAR(13)+CHAR(10),
        @cols, CHAR(13)+CHAR(10),
        N');'
    );

    -- ── Ausfuehren ─────────────────────────────────────────
    EXEC(@drop_in);
    EXEC(@crt_in);
    PRINT 'Eingangs-Tabelle erstellt: dbo.' + @dbo_table_in;

    EXEC(@drop_out);
    EXEC(@crt_out);
    PRINT 'Ausgangs-Tabelle erstellt: dbo.' + @dbo_table_out;

    FETCH NEXT FROM cur INTO @high_value;
END;

CLOSE cur;
DEALLOCATE cur;
DROP TABLE #partitions;

PRINT 'Fertig - alle dbo._in und dbo._out Tabellen wurden erfolgreich erstellt.';