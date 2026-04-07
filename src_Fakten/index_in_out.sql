/* =============================================================================
   SCHRITT 6: Clustered Index auf _in UND _out Tabellen
  
   =============================================================================*/
DECLARE @Verfahren    sysname = N'tf_lst_abrechn_btr_kug';
DECLARE @SchemaName   sysname = N'dbo';

DECLARE @FactTable          sysname;
DECLARE @PartitionColumnRaw nvarchar(200);
DECLARE @PartitionColumn    sysname;
DECLARE @Compression        nvarchar(50);
DECLARE @IndexType          varchar(20);
DECLARE @NcciFlag           varchar(20);

/* --- Parameter einmalig laden --- */
SELECT
    @FactTable          = Faktentabelle,
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
    Faktenpartitionsspalte,
    Faktenkomprimierung,
    FaktenClusteredIndex,
    FaktenNccIndex
)) AS pvt;

-- Partitionsspalte bereinigen
SET @PartitionColumn = CASE WHEN CHARINDEX('|',@PartitionColumnRaw) > 0
                            THEN LEFT(@PartitionColumnRaw, CHARINDEX('|',@PartitionColumnRaw)-1)
                            ELSE @PartitionColumnRaw END;

-- Pflichtprüfungen
IF @FactTable       IS NULL BEGIN RAISERROR('Faktentabelle nicht gefunden: %s',    16,1,@Verfahren); RETURN; END
IF @PartitionColumn IS NULL BEGIN RAISERROR('Partitionsspalte nicht gefunden: %s', 16,1,@Verfahren); RETURN; END

PRINT '=== Parameter geladen:';
PRINT '    Faktentabelle:    ' + @FactTable;
PRINT '    Partitionsspalte: ' + @PartitionColumn;
PRINT '    Komprimierung:    ' + ISNULL(@Compression, 'keine');
PRINT '    Index-Typ:        ' + ISNULL(@IndexType,   'kein Index');
PRINT '    NCCI:             ' + @NcciFlag;

/* =============================================================================
   Cursor über alle _in UND _out Tabellen
   =============================================================================*/
DECLARE @sql        nvarchar(max);
DECLARE @ColList    nvarchar(max);
DECLARE @table_name sysname;
DECLARE @CICols     nvarchar(max);

-- CI Spalten einmalig lesen (nur bei CI relevant)
IF @IndexType = 'TRUE'
BEGIN
    SELECT @CICols = STUFF((
        SELECT ', ' + QUOTENAME(c.name)
        FROM sys.index_columns ic
        JOIN sys.columns c
            ON  c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE ic.object_id = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable))
          AND ic.index_id  = 1
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'');

    IF @CICols IS NULL
        SET @CICols = QUOTENAME(@PartitionColumn);

    PRINT '    CI Spalten: ' + @CICols;
END

-- Cursor über BEIDE _in und _out Tabellen
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT name
    FROM sys.tables
    WHERE schema_id = SCHEMA_ID(@SchemaName)
      AND (
            name LIKE LOWER(@FactTable) + '_in_%'
         OR name LIKE LOWER(@FactTable) + '_out_%'
          )
    ORDER BY name;

OPEN cur;
FETCH NEXT FROM cur INTO @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '=== Verarbeite: ' + @table_name;

    /* -------------------------------------------------------------------------
       CI oder CCI anlegen — mit Existenzprüfung
       -------------------------------------------------------------------------*/
    IF @IndexType = 'TRUE'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@table_name))
              AND name = 'CI_' + @table_name
        )
            PRINT '    CI bereits vorhanden → übersprungen: CI_' + @table_name;
        ELSE
        BEGIN
            SET @sql =
                'CREATE CLUSTERED INDEX ' + QUOTENAME('CI_' + @table_name) +
                ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@table_name) +
                ' (' + @CICols + ')' +
                ' WITH (FILLFACTOR=100, SORT_IN_TEMPDB=ON);';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    CI angelegt: CI_' + @table_name;
            END TRY
            BEGIN CATCH
                PRINT '    FEHLER CI: ' + ERROR_MESSAGE(); THROW;
            END CATCH;
        END
    END
    ELSE IF @IndexType = 'CCI'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@table_name))
              AND name = 'CCI_' + @table_name
        )
            PRINT '    CCI bereits vorhanden → übersprungen: CCI_' + @table_name;
        ELSE
        BEGIN
            SET @sql =
                'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME('CCI_' + @table_name) +
                ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@table_name) + ';';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    CCI angelegt: CCI_' + @table_name;
            END TRY
            BEGIN CATCH
                PRINT '    FEHLER CCI: ' + ERROR_MESSAGE(); THROW;
            END CATCH;
        END
    END
    ELSE
        PRINT '    Kein gruppierter Index. Typ: ' + ISNULL(@IndexType,'<NULL>');

    /* -------------------------------------------------------------------------
       Komprimierung — nur auf _out und nur bei CI
       _in bekommt keine Komprimierung (nur Index für schnelles Lesen)
       -------------------------------------------------------------------------*/
    IF @table_name LIKE LOWER(@FactTable) + '_out_%'
       AND @Compression IN ('PAGE','ROW')
       AND @IndexType = 'TRUE'
    BEGIN
        SET @sql =
            'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@table_name) +
            ' REBUILD WITH (DATA_COMPRESSION = ' + @Compression + ');';
        BEGIN TRY
            EXEC(@sql);
            PRINT '    Komprimierung gesetzt: ' + @Compression;
        END TRY
        BEGIN CATCH
            PRINT '    FEHLER Komprimierung: ' + ERROR_MESSAGE(); THROW;
        END CATCH;
    END
    ELSE IF @table_name LIKE LOWER(@FactTable) + '_out_%'
         AND @IndexType = 'CCI'
        PRINT '    CCI aktiv → PAGE/ROW nicht anwendbar, CCI komprimiert intern.';
    ELSE IF @table_name LIKE LOWER(@FactTable) + '_in_%'
        PRINT '    _in Tabelle → keine Komprimierung gesetzt.';

    /* -------------------------------------------------------------------------
       NCCI — nur auf _out, nicht kombinierbar mit CCI
       -------------------------------------------------------------------------*/
    IF @table_name LIKE LOWER(@FactTable) + '_out_%'
       AND @NcciFlag = 'TRUE'
       AND @IndexType != 'CCI'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@table_name))
              AND name = 'NCCI_' + @table_name
        )
            PRINT '    NCCI bereits vorhanden → übersprungen.';
        ELSE
        BEGIN
            SET @ColList = NULL;
            SELECT @ColList = STRING_AGG(QUOTENAME(name), ',')
            FROM sys.columns
            WHERE object_id  = OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@table_name))
              AND is_computed = 0;

            SET @sql =
                'CREATE NONCLUSTERED COLUMNSTORE INDEX ' + QUOTENAME('NCCI_' + @table_name) +
                ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@table_name) +
                ' (' + @ColList + ');';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    NCCI angelegt: NCCI_' + @table_name;
            END TRY
            BEGIN CATCH
                PRINT '    FEHLER NCCI: ' + ERROR_MESSAGE(); THROW;
            END CATCH;
        END
    END
    ELSE IF @table_name LIKE LOWER(@FactTable) + '_out_%'
         AND @IndexType = 'CCI'
        PRINT '    NCCI nicht möglich → CCI bereits vorhanden.';

    PRINT '    → Abgeschlossen: ' + @table_name;
    FETCH NEXT FROM cur INTO @table_name;
END;

CLOSE cur;
DEALLOCATE cur;

PRINT '=== Schritt 6+7+8 erfolgreich abgeschlossen ===';
