/* =============================================================================
   PARAMETER
   =============================================================================*/
DECLARE @Database     sysname = N'dwh';
DECLARE @SchemaName   sysname = N'dbo';
DECLARE @Verfahren    sysname = N'tf_lst_abrechn_btr_kug';
DECLARE @Themengebiet sysname = N'PSD1_DM_STAT_LST_KUG';

/* =============================================================================
   Parameter aus Parametertabelle laden
   =============================================================================*/
DECLARE @FactTable          sysname;
DECLARE @PartitionColumnRaw nvarchar(200);
DECLARE @PartitionColumn    sysname;

SELECT
    @FactTable          = Faktentabelle,
    @PartitionColumnRaw = Faktenpartitionsspalte
FROM (
    SELECT Parameter, Wert
    FROM msi_dm_lst_sgbiii.dbo.tm_msi_dm_lst_sgbiii_param
    WHERE Verfahren = @Verfahren
) AS src
PIVOT (MAX(Wert) FOR Parameter IN (Faktentabelle, Faktenpartitionsspalte)) AS pvt;

-- Partitionsspalte bereinigen
SET @PartitionColumn = CASE WHEN CHARINDEX('|',@PartitionColumnRaw) > 0
                            THEN LEFT(@PartitionColumnRaw, CHARINDEX('|',@PartitionColumnRaw)-1)
                            ELSE @PartitionColumnRaw END;

IF @FactTable       IS NULL BEGIN RAISERROR('Faktentabelle nicht gefunden: %s',    16,1,@Verfahren); RETURN; END
IF @PartitionColumn IS NULL BEGIN RAISERROR('Partitionsspalte nicht gefunden: %s', 16,1,@Verfahren); RETURN; END

DECLARE @PF        sysname = 'PF_' + @PartitionColumn + '_' + @FactTable;
DECLARE @PS        sysname = 'PS_' + @PartitionColumn + '_' + @FactTable;
DECLARE @Filegroup sysname;
DECLARE @sql       nvarchar(max);

-- Standard-Dateigruppe ermitteln
SELECT @Filegroup = name FROM sys.filegroups WHERE is_default = 1;
IF @Filegroup IS NULL BEGIN RAISERROR('Standard-Dateigruppe nicht gefunden.', 16,1); RETURN; END

PRINT '=== Faktentabelle: ' + @FactTable + ' | Partitionsspalte: ' + @PartitionColumn + ' | Dateigruppe: ' + @Filegroup;
PRINT '=== Partitionsfunktion: ' + @PF;

/* =============================================================================
   High-Values laden
   Schritt 1: Rohdaten ohne CAST laden (LONG Spalte darf nicht auf Oracle gefiltert werden)
   Schritt 2: Lokal casten und filtern
   =============================================================================*/
IF OBJECT_ID('tempdb..#highvalues_raw') IS NOT NULL DROP TABLE #highvalues_raw;

-- Nur VARCHAR Spalten in WHERE → Pushdown funktioniert
-- Kein CAST, kein Filter auf HIGH_VALUE → sonst Oracle LONG Fehler
SELECT HIGH_VALUE
INTO #highvalues_raw
FROM [DWH].[ext].[v_all_tab_partitions]
WHERE TABLE_NAME  = 'TF_LST_ABRECHN_BTR_KUG'
  AND TABLE_OWNER = 'PSD1_DM_STAT_LST_KUG';

PRINT '=== Rohdaten geladen: ' + CAST(@@ROWCOUNT AS nvarchar(10));

-- Lokal casten und filtern (kein Oracle Zugriff mehr)
IF OBJECT_ID('tempdb..#highvalues') IS NOT NULL DROP TABLE #highvalues;

SELECT CAST(HIGH_VALUE AS int) AS PartitionValue
INTO #highvalues
FROM #highvalues_raw
WHERE ISNUMERIC(HIGH_VALUE) = 1;  -- nur numerische Werte, PRT_FIRST wird automatisch ausgeschlossen

DROP TABLE #highvalues_raw;

PRINT '=== Partitionsgrenzwerte nach lokaler Filterung: ' + CAST(@@ROWCOUNT AS nvarchar(10));

/* =============================================================================
   Cursor: Für jeden High-Value Partitionsgrenze anlegen
   =============================================================================*/
DECLARE @PartitionValue   int;
DECLARE @PartitionTreffer int;
DECLARE @PartitionLeer    int;
DECLARE @PartitionName    int;
DECLARE @PartitionID      int;
DECLARE @PartitionID_new  int;
DECLARE @Compression      nvarchar(20);
DECLARE @TmpTable         sysname;
DECLARE @ColDef           nvarchar(max);
DECLARE @CICols           nvarchar(max);
DECLARE @AllCols          nvarchar(max);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT PartitionValue FROM #highvalues ORDER BY PartitionValue;

OPEN cur;
FETCH NEXT FROM cur INTO @PartitionValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '=== Verarbeite Partitionsgrenzwert: ' + CAST(@PartitionValue AS nvarchar(20));

    -- Variablen zurücksetzen
    SELECT @PartitionTreffer=NULL, @PartitionLeer=NULL, @PartitionName=NULL, @PartitionID=NULL;

    /* --- Partitionssituation ermitteln --- */
    SELECT
        @PartitionTreffer = MAX(CASE WHEN CONVERT(int,r.value) = @PartitionValue THEN 1 ELSE 0 END),
        @PartitionLeer    = MAX(CASE WHEN p.rows = 0 THEN 1 ELSE 0 END),
        @PartitionName    = MAX(ISNULL(CONVERT(int,r.value), 2147483647)),
        @PartitionID      = MAX(p.partition_number)
    FROM       sys.indexes i
    INNER JOIN sys.tables t   ON i.object_id = t.object_id
    INNER JOIN sys.partitions p
               ON i.object_id = p.object_id
              AND i.index_id  = p.index_id
              AND p.index_id  < 2
    INNER JOIN sys.data_spaces d  ON i.data_space_id = d.data_space_id
    LEFT  JOIN sys.partition_schemes s   ON d.name = s.name
    LEFT  JOIN sys.partition_functions f ON s.function_id = f.function_id
    LEFT  JOIN sys.partition_range_values r
               ON r.function_id = f.function_id
              AND r.boundary_id + f.boundary_value_on_right = p.partition_number
    LEFT  JOIN sys.partition_range_values v
               ON v.function_id = f.function_id
              AND CONVERT(int,v.value) < ISNULL(CONVERT(int,r.value), 2147483647)
    WHERE t.type = 'U'
      AND t.schema_id = SCHEMA_ID(@SchemaName)
      AND t.name = @FactTable
    GROUP BY r.value, p.partition_number, p.rows
    HAVING @PartitionValue >  ISNULL(MAX(CONVERT(int,v.value)), -2147483648)
       AND @PartitionValue <= ISNULL(CONVERT(int,r.value),       2147483647);

    /* --- Grenze bereits vorhanden → überspringen --- */
    IF @PartitionTreffer = 1
    BEGIN
        PRINT '  -> Grenze bereits vorhanden → übersprungen.';
        FETCH NEXT FROM cur INTO @PartitionValue;
        CONTINUE;
    END

    /* --- Leere Partition → direkter SPLIT --- */
    IF @PartitionLeer = 1
    BEGIN
        SET @sql =
            'ALTER PARTITION SCHEME ' + QUOTENAME(@PS) + ' NEXT USED ' + QUOTENAME(@Filegroup) + ';' +
            'ALTER PARTITION FUNCTION ' + QUOTENAME(@PF) + '() SPLIT RANGE (' + CAST(@PartitionValue AS varchar(20)) + ');';
        BEGIN TRY
            EXEC(@sql);
            PRINT '  -> Direkter SPLIT erfolgreich: ' + CAST(@PartitionValue AS nvarchar(20));
        END TRY
        BEGIN CATCH
            PRINT 'FEHLER beim SPLIT: ' + ERROR_MESSAGE(); THROW;
        END CATCH;
        FETCH NEXT FROM cur INTO @PartitionValue;
        CONTINUE;
    END

    /* --- Nicht-leere Partition → SWITCH OUT / SPLIT / SWITCH IN --- */
    PRINT '  -> Nicht-leere Partition → SWITCH OUT / SPLIT / SWITCH IN';

    -- Komprimierung ermitteln
    SET @Compression = NULL;
    SELECT TOP 1 @Compression = p.data_compression_desc
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id=i.object_id AND p.index_id=i.index_id
    JOIN sys.tables t  ON t.object_id=p.object_id
    WHERE t.schema_id=SCHEMA_ID(@SchemaName) AND t.name=@FactTable AND p.partition_number=1;
    IF @Compression NOT IN ('PAGE','ROW') SET @Compression = NULL;

    -- Hilfstabellenname
    SET @TmpTable = @FactTable + '_tmp_' + CAST(@PartitionName AS varchar(20));

    -- Hilfstabelle löschen falls vorhanden
    SET @sql = 'IF OBJECT_ID('''+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+''',''U'') IS NOT NULL DROP TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+';';
    EXEC(@sql);

    -- Spaltendefinition aufbauen
    SET @ColDef = NULL;
    SELECT @ColDef = STUFF((
        SELECT ', ' + QUOTENAME(c.name) + ' ' +
               CASE
                 WHEN y.name IN ('char','nchar','binary')             THEN y.name+'('+LTRIM(STR(c.max_length))+')'
                 WHEN y.name IN ('varchar','nvarchar','varbinary')    THEN y.name+'('+CASE WHEN c.max_length=-1 THEN 'max' ELSE LTRIM(STR(c.max_length)) END+')'
                 WHEN y.name IN ('decimal','numeric')                 THEN y.name+'('+LTRIM(STR(c.precision))+','+LTRIM(STR(c.scale))+')'
                 WHEN y.name IN ('datetime2','datetimeoffset','time') THEN y.name+'('+LTRIM(STR(c.scale))+')'
                 ELSE y.name
               END + ' ' + CASE WHEN c.is_nullable=1 THEN 'NULL' ELSE 'NOT NULL' END
        FROM sys.columns c
        JOIN sys.types y ON c.user_type_id=y.user_type_id
        WHERE c.object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND c.is_computed=0
        ORDER BY c.column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');

    -- Hilfstabelle anlegen
    SET @sql = 'CREATE TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+' ('+@ColDef+')'+
               CASE WHEN @Compression IS NOT NULL THEN ' WITH (DATA_COMPRESSION='+@Compression+')' ELSE '' END+';';
    BEGIN TRY EXEC(@sql); PRINT '  -> Hilfstabelle angelegt: ' + @TmpTable;
    END TRY BEGIN CATCH PRINT 'FEHLER Hilfstabelle: ' + ERROR_MESSAGE(); THROW; END CATCH;

    -- CI auf Hilfstabelle (falls vorhanden)
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND type=1)
    BEGIN
        SET @CICols = NULL;
        SELECT @CICols = STUFF((
            SELECT ', '+QUOTENAME(c.name) FROM sys.index_columns ic
            JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
            WHERE ic.object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND ic.index_id=1
            ORDER BY ic.key_ordinal FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');
        IF @CICols IS NOT NULL
        BEGIN
            SET @sql = 'CREATE CLUSTERED INDEX CI_'+@TmpTable+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+'('+@CICols+') WITH (SORT_IN_TEMPDB=ON);';
            EXEC(@sql); PRINT '  -> CI angelegt.';
        END
    END

    -- NCCI auf Hilfstabelle (falls vorhanden)
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND type=6)
    BEGIN
        SET @AllCols = NULL;
        SELECT @AllCols = STUFF((
            SELECT ', '+QUOTENAME(name) FROM sys.columns
            WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND is_computed=0
            ORDER BY column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');
        SET @sql = 'CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_'+@TmpTable+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+' ('+@AllCols+');';
        EXEC(@sql); PRINT '  -> NCCI angelegt.';
    END

    -- CCI auf Hilfstabelle (falls vorhanden)
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND type=5)
    BEGIN
        SET @sql = 'CREATE CLUSTERED COLUMNSTORE INDEX CCI_'+@TmpTable+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+';';
        EXEC(@sql); PRINT '  -> CCI angelegt.';
    END

    -- SWITCH OUT
    SET @sql = 'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
               ' SWITCH PARTITION '+CAST(@PartitionID AS varchar(20))+
               ' TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+';';
    BEGIN TRY EXEC(@sql); PRINT '  -> SWITCH OUT erfolgreich.';
    END TRY BEGIN CATCH PRINT 'FEHLER SWITCH OUT: ' + ERROR_MESSAGE(); THROW; END CATCH;

    -- SPLIT
    SET @sql =
        'ALTER PARTITION SCHEME '+QUOTENAME(@PS)+' NEXT USED '+QUOTENAME(@Filegroup)+';'+
        'ALTER PARTITION FUNCTION '+QUOTENAME(@PF)+'() SPLIT RANGE ('+CAST(@PartitionValue AS varchar(20))+');';
    BEGIN TRY EXEC(@sql); PRINT '  -> SPLIT erfolgreich.';
    END TRY BEGIN CATCH PRINT 'FEHLER SPLIT: ' + ERROR_MESSAGE(); THROW; END CATCH;

    -- Neue Partitions-ID ermitteln
    SELECT @PartitionID_new = sprv.boundary_id
    FROM sys.partition_functions spf
    JOIN sys.partition_range_values sprv ON sprv.function_id=spf.function_id
    WHERE spf.name=@PF AND sprv.value=@PartitionName;
    PRINT '  -> Neue Partitions-ID: ' + ISNULL(CAST(@PartitionID_new AS nvarchar(20)),'NULL');

    -- CHECK-Constraint setzen
    SET @sql =
        'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+
        ' WITH CHECK ADD CONSTRAINT CK_'+@TmpTable+
        ' CHECK ('+QUOTENAME(@PartitionColumn)+' <= '+CAST(@PartitionName AS varchar(20))+
        ' AND '+QUOTENAME(@PartitionColumn)+' > '+CAST(@PartitionValue AS varchar(20))+
        ' AND '+QUOTENAME(@PartitionColumn)+' IS NOT NULL);';
    BEGIN TRY EXEC(@sql); PRINT '  -> CHECK-Constraint gesetzt.';
    END TRY BEGIN CATCH PRINT 'FEHLER CHECK-Constraint: ' + ERROR_MESSAGE(); THROW; END CATCH;

    -- SWITCH IN
    SET @sql =
        'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+
        ' SWITCH TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
        ' PARTITION '+CAST(@PartitionID_new AS varchar(20))+';';
    BEGIN TRY EXEC(@sql); PRINT '  -> SWITCH IN erfolgreich.';
    END TRY BEGIN CATCH PRINT 'FEHLER SWITCH IN: ' + ERROR_MESSAGE(); THROW; END CATCH;

    -- Hilfstabelle löschen
    SET @sql = 'DROP TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpTable)+';';
    BEGIN TRY EXEC(@sql); PRINT '  -> Hilfstabelle gelöscht.';
    END TRY BEGIN CATCH PRINT 'WARNUNG Hilfstabelle: ' + ERROR_MESSAGE(); END CATCH;

    PRINT '  -> Partitionsgrenze erfolgreich angelegt: ' + CAST(@PartitionValue AS nvarchar(20));
    FETCH NEXT FROM cur INTO @PartitionValue;
END;

CLOSE cur;
DEALLOCATE cur;
DROP TABLE #highvalues;

PRINT '=== Alle Partitionsgrenzen erfolgreich angelegt ===';