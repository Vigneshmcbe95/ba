/* =============================================================================
   PARAMETER
   =============================================================================*/
DECLARE @Verfahren    sysname = N'tf_lst_abrechn_btr_kug';
DECLARE @SchemaName   sysname = N'dbo';
DECLARE @Themengebiet sysname = N'PSD1_DM_STAT_LST_KUG';

DECLARE @FactTable          sysname;
DECLARE @TemplateTable      sysname;
DECLARE @PartitionColumnRaw nvarchar(200);
DECLARE @PartitionColumn    sysname;
DECLARE @Compression        nvarchar(50);
DECLARE @IndexType          varchar(20);
DECLARE @NcciFlag           varchar(20);
DECLARE @Filegroup          sysname;
DECLARE @sql                nvarchar(max);
DECLARE @sql_cols           nvarchar(max);
DECLARE @ColumnList         nvarchar(max);

/* --- Alle Parameter per PIVOT in einem Schritt laden --- */
SELECT
    @FactTable          = Faktentabelle,
    @TemplateTable      = FaktentabelleTemplate,
    @PartitionColumnRaw = Faktenpartitionsspalte,
    @Compression        = UPPER(Faktenkomprimierung),
    @IndexType          = UPPER(FaktenClusteredIndex),
    @NcciFlag           = UPPER(ISNULL(FaktenNccIndex,'FALSE'))
FROM (
    SELECT Parameter, Wert
    FROM msi_dm_lst_sgbiii.dbo.tm_msi_dm_lst_sgbiii_param
    WHERE Verfahren = @Verfahren
) AS src
PIVOT (MAX(Wert) FOR Parameter IN (
    Faktentabelle,
    FaktentabelleTemplate,
    Faktenpartitionsspalte,
    Faktenkomprimierung,
    FaktenClusteredIndex,
    FaktenNccIndex
)) AS pvt;

-- Partitionsspalte bereinigen
SET @PartitionColumn = CASE WHEN CHARINDEX('|',@PartitionColumnRaw) > 0
                            THEN LEFT(@PartitionColumnRaw, CHARINDEX('|',@PartitionColumnRaw)-1)
                            ELSE @PartitionColumnRaw END;

-- Standard-Dateigruppe ermitteln
SELECT @Filegroup = name FROM sys.filegroups WHERE is_default = 1;

-- Pflichtprüfungen
IF @FactTable       IS NULL BEGIN RAISERROR('Faktentabelle nicht gefunden: %s',     16,1,@Verfahren); RETURN; END
IF @TemplateTable   IS NULL BEGIN RAISERROR('Vorlagentabelle nicht gefunden: %s',   16,1,@Verfahren); RETURN; END
IF @PartitionColumn IS NULL BEGIN RAISERROR('Partitionsspalte nicht gefunden: %s',  16,1,@Verfahren); RETURN; END
IF @Filegroup       IS NULL BEGIN RAISERROR('Standard-Dateigruppe nicht gefunden.', 16,1);             RETURN; END

PRINT '=== Parameter: ' + @FactTable + ' | ' + @PartitionColumn + ' | ' + @Filegroup + ' | ' + ISNULL(@Compression,'keine') + ' | ' + ISNULL(@IndexType,'kein Index') + ' | Themengebiet: ' + @Themengebiet;

/* =============================================================================
   Partitionsnamen ableiten
   =============================================================================*/
DECLARE @PF sysname = 'PF_' + @PartitionColumn + '_' + @FactTable;
DECLARE @PS sysname = 'PS_' + @PartitionColumn + '_' + @FactTable;

/* =============================================================================
   Vorhandene Faktentabelle und Partitionierung löschen
   Reihenfolge: Tabelle → Schema → Funktion
   =============================================================================*/

-- 1. Tabelle löschen
IF OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable),'U') IS NOT NULL
BEGIN
    SET @sql = 'DROP TABLE ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+';';
    EXEC(@sql);
    PRINT '=== Vorhandene Faktentabelle gelöscht: ' + @FactTable;
END
ELSE
    PRINT '=== Faktentabelle nicht vorhanden: ' + @FactTable;

-- 2. Partitionsschema löschen
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name=@PS)
BEGIN
    SET @sql = 'DROP PARTITION SCHEME ' + QUOTENAME(@PS);
    EXEC(@sql);
    PRINT '=== Partitionsschema gelöscht: ' + @PS;
END
ELSE
    PRINT '=== Partitionsschema nicht vorhanden: ' + @PS;

-- 3. Partitionsfunktion löschen
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name=@PF)
BEGIN
    SET @sql = 'DROP PARTITION FUNCTION ' + QUOTENAME(@PF);
    EXEC(@sql);
    PRINT '=== Partitionsfunktion gelöscht: ' + @PF;
END
ELSE
    PRINT '=== Partitionsfunktion nicht vorhanden: ' + @PF;

/* =============================================================================
   Partitionsfunktion und Partitionsschema neu anlegen
   =============================================================================*/
SET @sql = 'CREATE PARTITION FUNCTION ' + QUOTENAME(@PF) + ' (INT) AS RANGE LEFT FOR VALUES (0);';
EXEC(@sql);

SET @sql = 'CREATE PARTITION SCHEME ' + QUOTENAME(@PS) +
           ' AS PARTITION ' + QUOTENAME(@PF) +
           ' ALL TO (' + QUOTENAME(@Filegroup) + ');';
EXEC(@sql);
PRINT '=== Partitionierung angelegt: ' + @PF + ' / ' + @PS + ' auf Dateigruppe: ' + @Filegroup;

/* =============================================================================
   Spaltenliste aus Vorlagentabelle ermitteln
   columns_ext verwenden (enthält "COL datatype NULL" → korrekt für CREATE TABLE)
   Gleiche Methode wie ext-Tabellen-Erstellung: sp_executesql mit OUTPUT
   =============================================================================*/
SET @sql_cols = CONCAT(
    N'SELECT @cols = ''('' + STRING_AGG(CAST(columns_ext AS nvarchar(max)), '','' + CHAR(13)+CHAR(10)) ',
    N'WITHIN GROUP (ORDER BY colno) + '')'' ',
    N'FROM dwh.dbo.', @TemplateTable, ' ',
    N'WHERE tabname      = ''', UPPER(@FactTable), ''' ',
    N'  AND themengebiet = ''', @Themengebiet, ''';'
);

EXEC sp_executesql
    @sql_cols,
    N'@cols nvarchar(max) OUTPUT',
    @cols = @ColumnList OUTPUT;

IF @ColumnList IS NULL BEGIN RAISERROR('Spaltenliste konnte nicht ermittelt werden: %s',16,1,@TemplateTable); RETURN; END
PRINT '=== Spaltenliste ermittelt aus Vorlagentabelle: ' + @TemplateTable;

/* =============================================================================
   Faktentabelle neu anlegen
   =============================================================================*/
SET @sql = 'CREATE TABLE ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable) + ' ' +
           @ColumnList +
           ' ON ' + @PS + '(' + @PartitionColumn + ')' +
           CASE WHEN @Compression IN ('PAGE','ROW')
                THEN ' WITH (DATA_COMPRESSION='+@Compression+')'
                ELSE '' END + ';';
EXEC(@sql);

-- Sperreskalierung AUTO setzen
SET @sql = 'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+' SET (LOCK_ESCALATION=AUTO);';
EXEC(@sql);
PRINT '=== Faktentabelle angelegt: ' + @FactTable;

/* =============================================================================
   Gruppierter Index (CI oder CCI)
   =============================================================================*/
DECLARE @CIName  sysname = 'CI_'  + @FactTable;
DECLARE @CCIName sysname = 'CCI_' + @FactTable;

IF @IndexType = 'TRUE'
BEGIN
    -- Gruppierter Zeilenindex
    SET @sql =
        'CREATE CLUSTERED INDEX ' + QUOTENAME(@CIName) +
        ' ON ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
        ' ('+QUOTENAME(@PartitionColumn)+')' +
        ' WITH (FILLFACTOR=100, SORT_IN_TEMPDB=ON' +
        CASE WHEN @Compression IN ('PAGE','ROW')
             THEN ', DATA_COMPRESSION='+@Compression ELSE '' END + ')' +
        ' ON ' + @PS + '(' + @PartitionColumn + ');';
    EXEC(@sql);
    PRINT '=== Gruppierter Zeilenindex angelegt: ' + @CIName;
END
ELSE IF @IndexType = 'CCI'
BEGIN
    -- Gruppierter Columnstore-Index
    SET @sql =
        'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(@CCIName) +
        ' ON ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
        ' ON ' + @PS + '(' + @PartitionColumn + ');';
    EXEC(@sql);
    PRINT '=== Gruppierter Columnstore-Index angelegt: ' + @CCIName;
END
ELSE
    PRINT '=== Kein gruppierter Index angelegt. Index-Typ: ' + ISNULL(@IndexType,'<NULL>');

/* =============================================================================
   Nicht-gruppierter Columnstore-Index (NCCI)
   =============================================================================*/
IF @NcciFlag = 'TRUE'
BEGIN
    DECLARE @NCCIName sysname = 'NCCI_' + @FactTable;
    DECLARE @ColList2 nvarchar(max);

    -- Alle Spalten der neu angelegten Faktentabelle ermitteln
    SELECT @ColList2 = STRING_AGG(QUOTENAME(name),',')
    FROM sys.columns
    WHERE object_id  = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable))
      AND is_computed = 0;

    SET @sql =
        'CREATE NONCLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(@NCCIName) +
        ' ON ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
        ' ('+@ColList2+');';
    EXEC(@sql);
    PRINT '=== Nicht-gruppierter Columnstore-Index angelegt: ' + @NCCIName;
END
ELSE
    PRINT '=== Kein NCCI angelegt. FaktenNccIndex = ' + @NcciFlag;

PRINT '=== Skript erfolgreich abgeschlossen ===';