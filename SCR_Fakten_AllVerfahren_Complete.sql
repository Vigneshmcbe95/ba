/* =============================================================================
   COMBINED SCRIPT: Fakten pipeline for ALL Verfahren
   Source control : [msi_dm_lst_sgbii].[dbo].[tm_steuerlistenfile_Fakten]

   STEPS PER VERFAHREN:
     1.  Resolve WHERE clause from tabname_filter
     2.  Create Template table
     3.  Create partitioned Fakten table
     4.  Extend partition boundaries
     5.  Create external tables per partition  (ext.table_HV)
     6.  Create staging _in / _out tables
     7.  Create indexes on _in / _out
     8.  DATA COPY: SELECT * INTO _in FROM ext.table WHERE <where_klausel>
     9.  PARTITION SWITCH: _in → FactTable, cleanup _out / _in

   OUTPUT: SELECT result = CSV with counters per row
     total_in_control | processed_ok | object_not_found | failed
   =============================================================================*/

SET NOCOUNT ON;
SET XACT_ABORT OFF;

-- ============================================================================
--  SUMMARY TABLE
-- ============================================================================
IF OBJECT_ID('tempdb..#summary') IS NOT NULL DROP TABLE #summary;
CREATE TABLE #summary (
    Verfahren        nvarchar(200),
    Themengebiet     nvarchar(200),
    tabname_filter   nvarchar(500),
    where_klausel    nvarchar(max),
    rows_loaded      bigint,
    Status           nvarchar(50),    -- OK | OBJECT_NOT_FOUND | FAILED
    Bemerkung        nvarchar(max),
    ref_datum        datetime
);

-- ============================================================================
--  COUNTERS
-- ============================================================================
DECLARE @cnt_total     int      = 0;
DECLARE @cnt_ok        int      = 0;
DECLARE @cnt_not_found int      = 0;
DECLARE @cnt_failed    int      = 0;
DECLARE @refDatum      datetime = GETDATE();

-- ============================================================================
--  SHARED WORK VARIABLES
-- ============================================================================
DECLARE @Verfahren          nvarchar(200);
DECLARE @Themengebiet       nvarchar(200);
DECLARE @tabname_filter     nvarchar(500);
DECLARE @FileName           nvarchar(500);
DECLARE @where_klausel      nvarchar(max);
DECLARE @partition_col      nvarchar(50);
DECLARE @sql                nvarchar(max);
DECLARE @sql_cols           nvarchar(max);
DECLARE @ColumnList         nvarchar(max);
DECLARE @FactTable          nvarchar(200);
DECLARE @TemplateTable      nvarchar(200);
DECLARE @PartitionColumnRaw nvarchar(200);
DECLARE @PartitionColumn    nvarchar(200);
DECLARE @Compression        nvarchar(50);
DECLARE @IndexType          nvarchar(20);
DECLARE @NcciFlag           nvarchar(20);
DECLARE @Filegroup          nvarchar(200);
DECLARE @PF                 nvarchar(300);
DECLARE @PS                 nvarchar(300);
DECLARE @SchemaName         nvarchar(50)  = N'dbo';
DECLARE @ExtSchema          nvarchar(50)  = N'ext';
DECLARE @ErrMsg             nvarchar(max);
DECLARE @Status             nvarchar(50);
DECLARE @Bemerkung          nvarchar(max);
DECLARE @rows_loaded        bigint;

-- ============================================================================
--  LOAD ALL ROWS FROM CONTROL TABLE
-- ============================================================================
IF OBJECT_ID('tempdb..#control') IS NOT NULL DROP TABLE #control;

SELECT
    UPPER(LTRIM(RTRIM(tabelle)))       AS Verfahren,
    UPPER(LTRIM(RTRIM(themengebiet)))  AS Themengebiet,
    LTRIM(RTRIM(tabname_filter))       AS tabname_filter,
    LTRIM(RTRIM(FILE_NAME))            AS FileName
INTO #control
FROM [msi_dm_lst_sgbii].[dbo].[tm_steuerlistenfile_Fakten]
WHERE tabelle        IS NOT NULL
  AND themengebiet   IS NOT NULL
  AND tabname_filter IS NOT NULL;

SELECT @cnt_total = COUNT(*) FROM #control;
PRINT '=== Total rows in tm_steuerlistenfile_Fakten: ' + CAST(@cnt_total AS nvarchar(20));

-- ============================================================================
--  MAIN CURSOR
-- ============================================================================
DECLARE cur_main CURSOR LOCAL FAST_FORWARD FOR
    SELECT Verfahren, Themengebiet, tabname_filter, FileName
    FROM   #control
    ORDER  BY Verfahren, tabname_filter;

OPEN cur_main;
FETCH NEXT FROM cur_main INTO @Verfahren, @Themengebiet, @tabname_filter, @FileName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @where_klausel = NULL;
    SET @rows_loaded   = 0;
    SET @Status        = 'OK';
    SET @Bemerkung     = '';
    SET @partition_col = 'MON_ID';

    PRINT '';
    PRINT '============================================================';
    PRINT '=== Verfahren    : ' + @Verfahren;
    PRINT '=== Themengebiet : ' + @Themengebiet;
    PRINT '=== Filter       : ' + @tabname_filter;

    BEGIN TRY

        /* ==================================================================
           STEP 1 — Resolve partition column (MON_ID vs MOW_ID)
        ================================================================== */
        IF EXISTS (
            SELECT 1 FROM sys.columns c
            JOIN sys.tables  t ON c.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id  = s.schema_id
            WHERE UPPER(t.name) = @Verfahren
              AND UPPER(c.name) = 'MOW_ID'
        )
            SET @partition_col = 'MOW_ID';
        ELSE
            SET @partition_col = 'MON_ID';

        PRINT '=== Partition column: ' + @partition_col;

        /* ==================================================================
           STEP 2 — Build WHERE clause from tabname_filter
        ================================================================== */
        DECLARE @f    nvarchar(500) = @tabname_filter;
        DECLARE @pcol nvarchar(50)  = @partition_col;
        DECLARE @resolved nvarchar(max) = NULL;

        -- ── Case 5/6: YYYYMM(:MONID(n1),:MONID(n2),...) ─────────────────
        IF PATINDEX('%YYYYMM(%', @f) > 0 AND PATINDEX('%:MONID(%', @f) > 0
        BEGIN
            DECLARE @inner    nvarchar(500);
            DECLARE @istart   int = PATINDEX('%YYYYMM(%', @f) + LEN('YYYYMM(') - 1;
            SET @inner = SUBSTRING(@f, @istart + 1, LEN(@f) - @istart);
            IF RIGHT(RTRIM(@inner),1) = ')' SET @inner = LEFT(@inner, LEN(@inner)-1);

            DECLARE @in_list   nvarchar(max) = '';
            DECLARE @rem       nvarchar(500) = @inner;
            DECLARE @mpos      int;
            DECLARE @nstr56    nvarchar(20);
            DECLARE @nval56    int;
            DECLARE @mval56    nvarchar(10);

            WHILE PATINDEX('%:MONID(%', @rem) > 0
            BEGIN
                SET @mpos   = PATINDEX('%:MONID(%', @rem);
                DECLARE @aft56 nvarchar(200) =
                    SUBSTRING(@rem, @mpos + LEN(':MONID(') - 1 + 1, 20);
                SET @nstr56 = LEFT(@aft56, CHARINDEX(')', @aft56) - 1);
                SET @nval56 = CAST(@nstr56 AS int);
                SET @mval56 = FORMAT(DATEADD(MONTH, @nval56, @refDatum), 'yyyyMM');
                IF LEN(@in_list) > 0 SET @in_list = @in_list + ',';
                SET @in_list = @in_list + '''' + @mval56 + '''';
                SET @rem = SUBSTRING(@rem, @mpos + LEN(':MONID(') + LEN(@nstr56) + 1, 500);
            END
            IF LEN(@in_list) > 0
                SET @resolved = 'WHERE ' + @pcol + ' IN (' + @in_list + ')';
        END

        -- ── Case 3/4 and 1/2: :MONID(n) with or without suffix ───────────
        ELSE IF PATINDEX('%:MONID(%', @f) > 0
        BEGIN
            DECLARE @mp34     int  = PATINDEX('%:MONID(%', @f);
            DECLARE @af34     nvarchar(200) =
                SUBSTRING(@f, @mp34 + LEN(':MONID(') - 1 + 1, 50);
            DECLARE @ns34     nvarchar(20) = LEFT(@af34, CHARINDEX(')', @af34) - 1);
            DECLARE @nv34     int          = CAST(@ns34 AS int);
            DECLARE @mo34     nvarchar(10) = FORMAT(DATEADD(MONTH, @nv34, @refDatum), 'yyyyMM');
            DECLARE @ap34     nvarchar(20) =
                SUBSTRING(@f, @mp34 + LEN(':MONID(') + LEN(@ns34) + 1, 10);
            DECLARE @sfx34    nvarchar(10) = '';
            DECLARE @si34     int = 1;
            WHILE @si34 <= LEN(@ap34)
                AND SUBSTRING(@ap34, @si34, 1) LIKE '[0-9]'
            BEGIN
                SET @sfx34 = @sfx34 + SUBSTRING(@ap34, @si34, 1);
                SET @si34  = @si34 + 1;
            END
            DECLARE @pv34 nvarchar(20) =
                CASE WHEN @sfx34 = '' OR @sfx34 = '00'
                     THEN @mo34
                     ELSE @mo34 + @sfx34 END;
            SET @resolved = 'WHERE ' + @pcol + ' = ''' + @pv34 + '''';
        END

        -- ── Case YEAR: :YEAR(n)ss ─────────────────────────────────────────
        ELSE IF PATINDEX('%:YEAR(%', @f) > 0
        BEGIN
            DECLARE @yrp  int = PATINDEX('%:YEAR(%', @f);
            DECLARE @yraf nvarchar(50)  =
                SUBSTRING(@f, @yrp + LEN(':YEAR(') - 1 + 1, 30);
            DECLARE @yrns nvarchar(10) = LEFT(@yraf, CHARINDEX(')', @yraf) - 1);
            DECLARE @yrnv int          = CAST(@yrns AS int);
            DECLARE @yryr nvarchar(6)  = FORMAT(DATEADD(YEAR, @yrnv, @refDatum), 'yyyy');
            DECLARE @yrap nvarchar(20) =
                SUBSTRING(@f, @yrp + LEN(':YEAR(') + LEN(@yrns) + 1, 10);
            DECLARE @yrsfx nvarchar(10) = '';
            DECLARE @yri   int = 1;
            WHILE @yri <= LEN(@yrap) AND SUBSTRING(@yrap, @yri, 1) LIKE '[0-9]'
            BEGIN SET @yrsfx = @yrsfx + SUBSTRING(@yrap, @yri, 1); SET @yri = @yri + 1; END
            SET @resolved = 'WHERE ' + @pcol + ' = ''' + @yryr + @yrsfx + '''';
        END

        -- ── Case YYYY: :YYYY(n)ss ─────────────────────────────────────────
        ELSE IF PATINDEX('%:YYYY(%', @f) > 0
        BEGIN
            DECLARE @yyp  int = PATINDEX('%:YYYY(%', @f);
            DECLARE @yyaf nvarchar(50)  =
                SUBSTRING(@f, @yyp + LEN(':YYYY(') - 1 + 1, 30);
            DECLARE @yyns nvarchar(10) = LEFT(@yyaf, CHARINDEX(')', @yyaf) - 1);
            DECLARE @yynv int          = CAST(@yyns AS int);
            DECLARE @yyyr nvarchar(6)  = FORMAT(DATEADD(YEAR, @yynv, @refDatum), 'yyyy');
            DECLARE @yyap nvarchar(20) =
                SUBSTRING(@f, @yyp + LEN(':YYYY(') + LEN(@yyns) + 1, 10);
            DECLARE @yysfx nvarchar(10) = '';
            DECLARE @yyi   int = 1;
            WHILE @yyi <= LEN(@yyap) AND SUBSTRING(@yyap, @yyi, 1) LIKE '[0-9]'
            BEGIN SET @yysfx = @yysfx + SUBSTRING(@yyap, @yyi, 1); SET @yyi = @yyi + 1; END
            SET @resolved = 'WHERE ' + @pcol + ' = ''' + @yyyr + @yysfx + '''';
        END

        -- ── Static YYYYMM: trailing _yyyymm ──────────────────────────────
        ELSE IF @f LIKE '%[_][12][0-9][0-9][0-9][0-1][0-9]'
             AND @f NOT LIKE '%[_][12][0-9][0-9][0-9][0-1][0-9][0-9][0-9]'
            SET @resolved = 'WHERE ' + @pcol + ' = ''' + RIGHT(@f, 6) + '''';

        -- ── Static YYYY: trailing _yyyy ───────────────────────────────────
        ELSE IF @f LIKE '%[_][12][0-9][0-9][0-9]'
            SET @resolved = 'WHERE ' + @pcol + ' LIKE ''' + RIGHT(@f, 4) + '%''';

        -- ── Case 7: no filter → full load ─────────────────────────────────
        ELSE
        BEGIN
            SET @resolved  = NULL;
            SET @Bemerkung = 'Case 7: no filter → full load';
        END

        SET @where_klausel = @resolved;
        PRINT '=== WHERE clause: ' + ISNULL(@where_klausel, 'NULL (full load)');

        /* ==================================================================
           STEP 3 — Check object exists (ext or dbo)
        ================================================================== */
        DECLARE @obj_exists bit = 0;
        IF EXISTS (SELECT 1 FROM sys.external_tables et
                   JOIN sys.schemas s ON et.schema_id = s.schema_id
                   WHERE UPPER(et.name) = @Verfahren)
            SET @obj_exists = 1;
        IF @obj_exists = 0 AND EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE UPPER(t.name) = @Verfahren)
            SET @obj_exists = 1;

        IF @obj_exists = 0
        BEGIN
            SET @Status    = 'OBJECT_NOT_FOUND';
            SET @Bemerkung = 'Not found in DB: ' + @Verfahren;
            SET @cnt_not_found += 1;
            PRINT '=== STATUS: OBJECT_NOT_FOUND';
            INSERT INTO #summary VALUES (
                @Verfahren, @Themengebiet, @tabname_filter,
                @where_klausel, 0, @Status, @Bemerkung, @refDatum);
            FETCH NEXT FROM cur_main INTO @Verfahren, @Themengebiet, @tabname_filter, @FileName;
            CONTINUE;
        END

        /* ==================================================================
           STEP 4 — Template table
        ================================================================== */
        SET @FactTable     = @Verfahren;
        SET @TemplateTable = @Verfahren + N'_TEMPLATE';

        -- Drop existing
        SET @sql = N'IF OBJECT_ID(N''dwh.dbo.' + QUOTENAME(@TemplateTable) +
                   N''',N''U'') IS NOT NULL DROP TABLE dwh.dbo.' + QUOTENAME(@TemplateTable) + N';';
        EXEC(@sql);

        SET @sql = N'
        SELECT DISTINCT
            ''' + @Themengebiet + N''' AS themengebiet,
            ''' + UPPER(@Verfahren)   + N''' AS tabname,
            ddl.colname, ddl.colno,
            CONCAT(CHAR(9), LOWER(ddl.COLNAME), '' = '',
                CASE WHEN ddl.IS_NULLABLE=0 THEN ''ISNULL('' ELSE '''' END,
                UPPER(ddl.COLNAME),
                CASE WHEN ddl.IS_NULLABLE=0 THEN '',0)'' ELSE '''' END
            ) AS columns_dbo,
            CONCAT(CHAR(9), UPPER(ddl.COLNAME), '' '', ddl.TYPNAME,
                CASE
                    WHEN ddl.TYPNAME IN (N''nvarchar'',N''varchar'',N''nchar'',N''char'')
                        THEN CONCAT(''('',ddl.COLLENGTH*4,'' COLLATE Latin1_General_100_CS_AS_SC_UTF8)'')
                    WHEN ddl.TYPNAME IN (N''decimal'',N''number'')
                        THEN CONCAT(''('',ddl.PRECISION,'','',ddl.SCALE,'')'')
                    ELSE ''''
                END,
                '' '',
                CASE WHEN ddl.IS_NULLABLE=0 THEN ''NOT NULL'' ELSE ''NULL'' END
            ) AS columns_ext
        INTO dwh.dbo.' + QUOTENAME(@TemplateTable) + N'
        FROM DWH.ext.vm_ddl_sql_server ddl
        WHERE CAST(ddl.THMNAME AS nvarchar(200)) COLLATE Latin1_General_100_CI_AS_SC_UTF8 = ''' + @Themengebiet  + N'''
          AND CAST(ddl.TABNAME  AS nvarchar(200)) COLLATE Latin1_General_100_CI_AS_SC_UTF8 = ''' + UPPER(@Verfahren) + N''';';
        EXEC(@sql);
        PRINT '=== Template table created: ' + @TemplateTable;

        /* ==================================================================
           STEP 5 — Load parameters
        ================================================================== */
        SET @FactTable = NULL;
        BEGIN TRY
            SELECT
                @FactTable          = Faktentabelle,
                @PartitionColumnRaw = Faktenpartitionsspalte,
                @Compression        = UPPER(Faktenkomprimierung),
                @IndexType          = UPPER(FaktenClusteredIndex),
                @NcciFlag           = UPPER(ISNULL(FaktenNccIndex,'FALSE'))
            FROM (
                SELECT Parameter, Wert
                FROM msi_dm_lst_sgbiii.dbo.tm_msi_dm_lst_sgbiii_param
                WHERE Verfahren = LOWER(@Verfahren)
            ) src
            PIVOT (MAX(Wert) FOR Parameter IN (
                Faktentabelle, Faktenpartitionsspalte,
                Faktenkomprimierung, FaktenClusteredIndex, FaktenNccIndex
            )) pvt;
        END TRY
        BEGIN CATCH
            PRINT '=== Param table not accessible, using defaults.';
        END CATCH

        -- Defaults if missing
        IF @FactTable          IS NULL SET @FactTable          = @Verfahren;
        IF @PartitionColumnRaw IS NULL SET @PartitionColumnRaw = @partition_col;
        IF @Compression        IS NULL SET @Compression        = 'PAGE';
        IF @IndexType          IS NULL SET @IndexType          = 'TRUE';
        IF @NcciFlag           IS NULL SET @NcciFlag           = 'FALSE';

        SET @PartitionColumn = CASE WHEN CHARINDEX('|',@PartitionColumnRaw) > 0
                                    THEN LEFT(@PartitionColumnRaw, CHARINDEX('|',@PartitionColumnRaw)-1)
                                    ELSE @PartitionColumnRaw END;

        SELECT @Filegroup = name FROM sys.filegroups WHERE is_default = 1;
        IF @Filegroup IS NULL SET @Filegroup = 'PRIMARY';

        SET @PF = 'PF_' + @PartitionColumn + '_' + @FactTable;
        SET @PS = 'PS_' + @PartitionColumn + '_' + @FactTable;

        PRINT '=== FactTable: ' + @FactTable + ' | PartCol: ' + @PartitionColumn;

        /* ==================================================================
           STEP 6 — Create partitioned Fakten table
        ================================================================== */
        IF OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable),'U') IS NOT NULL
        BEGIN
            SET @sql = 'DROP TABLE ' + QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+';';
            EXEC(@sql);
        END
        IF EXISTS (SELECT 1 FROM sys.partition_schemes   WHERE name=@PS)
        BEGIN SET @sql='DROP PARTITION SCHEME '  +QUOTENAME(@PS)+';'; EXEC(@sql); END
        IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name=@PF)
        BEGIN SET @sql='DROP PARTITION FUNCTION '+QUOTENAME(@PF)+';'; EXEC(@sql); END

        SET @sql='CREATE PARTITION FUNCTION '+QUOTENAME(@PF)+'(INT) AS RANGE LEFT FOR VALUES (0);';
        EXEC(@sql);
        SET @sql='CREATE PARTITION SCHEME '+QUOTENAME(@PS)+
                 ' AS PARTITION '+QUOTENAME(@PF)+' ALL TO ('+QUOTENAME(@Filegroup)+');';
        EXEC(@sql);

        SET @ColumnList = NULL;
        SET @sql_cols = CONCAT(
            N'SELECT @cols=''(''+STRING_AGG(CAST(columns_ext AS nvarchar(max)),'',''+CHAR(13)+CHAR(10))',
            N'WITHIN GROUP (ORDER BY colno)+'')'' ',
            N'FROM dwh.dbo.', QUOTENAME(@TemplateTable),
            N' WHERE tabname=''',UPPER(@FactTable),''' AND themengebiet=''',@Themengebiet,''';');
        EXEC sp_executesql @sql_cols, N'@cols nvarchar(max) OUTPUT', @cols=@ColumnList OUTPUT;

        IF @ColumnList IS NULL
            RAISERROR('Column list NULL for: %s', 16, 1, @FactTable);

        SET @sql = 'CREATE TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+' '+
                   @ColumnList+' ON '+@PS+'('+@PartitionColumn+')'+
                   CASE WHEN @Compression IN ('PAGE','ROW')
                        THEN ' WITH (DATA_COMPRESSION='+@Compression+')' ELSE '' END+';';
        EXEC(@sql);
        SET @sql='ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+' SET (LOCK_ESCALATION=AUTO);';
        EXEC(@sql);

        -- Clustered index
        IF @IndexType = 'TRUE'
        BEGIN
            SET @sql='CREATE CLUSTERED INDEX '+QUOTENAME('CI_'+@FactTable)+
                     ' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                     ' ('+QUOTENAME(@PartitionColumn)+')'+
                     ' WITH (FILLFACTOR=100,SORT_IN_TEMPDB=ON'+
                     CASE WHEN @Compression IN ('PAGE','ROW')
                          THEN ',DATA_COMPRESSION='+@Compression ELSE '' END+')'+
                     ' ON '+@PS+'('+@PartitionColumn+');';
            EXEC(@sql);
        END
        ELSE IF @IndexType='CCI'
        BEGIN
            SET @sql='CREATE CLUSTERED COLUMNSTORE INDEX '+QUOTENAME('CCI_'+@FactTable)+
                     ' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                     ' ON '+@PS+'('+@PartitionColumn+');';
            EXEC(@sql);
        END
        PRINT '=== Fact table created: ' + @FactTable;

        /* ==================================================================
           STEP 7 — Extend partition boundaries
        ================================================================== */
        IF OBJECT_ID('tempdb..#hv_raw') IS NOT NULL DROP TABLE #hv_raw;
        SELECT HIGH_VALUE INTO #hv_raw
        FROM [DWH].[ext].[v_all_tab_partitions]
        WHERE TABLE_NAME=UPPER(@Verfahren) AND TABLE_OWNER=@Themengebiet;

        IF OBJECT_ID('tempdb..#hv_int') IS NOT NULL DROP TABLE #hv_int;
        SELECT CAST(HIGH_VALUE AS int) AS PartitionValue
        INTO #hv_int FROM #hv_raw WHERE ISNUMERIC(HIGH_VALUE)=1;
        DROP TABLE #hv_raw;

        DECLARE @pv   int; DECLARE @pt  int; DECLARE @pl  int;
        DECLARE @pid  int; DECLARE @pid2 int; DECLARE @pnm int;
        DECLARE @TmpT nvarchar(300); DECLARE @CDef nvarchar(max);
        DECLARE @CIC  nvarchar(max); DECLARE @ACol nvarchar(max);

        DECLARE cur_pv CURSOR LOCAL FAST_FORWARD FOR
            SELECT PartitionValue FROM #hv_int ORDER BY PartitionValue;
        OPEN cur_pv; FETCH NEXT FROM cur_pv INTO @pv;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SELECT
                @pt  = MAX(CASE WHEN CONVERT(int,r.value)=@pv THEN 1 ELSE 0 END),
                @pl  = MAX(CASE WHEN p.rows=0 THEN 1 ELSE 0 END),
                @pnm = MAX(ISNULL(CONVERT(int,r.value),2147483647)),
                @pid = MAX(p.partition_number)
            FROM sys.indexes i
            JOIN sys.tables t    ON i.object_id=t.object_id
            JOIN sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id AND p.index_id<2
            JOIN sys.data_spaces d ON i.data_space_id=d.data_space_id
            LEFT JOIN sys.partition_schemes s   ON d.name=s.name
            LEFT JOIN sys.partition_functions f ON s.function_id=f.function_id
            LEFT JOIN sys.partition_range_values r
                ON r.function_id=f.function_id
               AND r.boundary_id+f.boundary_value_on_right=p.partition_number
            LEFT JOIN sys.partition_range_values v
                ON v.function_id=f.function_id
               AND CONVERT(int,v.value)<ISNULL(CONVERT(int,r.value),2147483647)
            WHERE t.type='U' AND t.schema_id=SCHEMA_ID(@SchemaName) AND t.name=@FactTable
            GROUP BY r.value, p.partition_number, p.rows
            HAVING @pv > ISNULL(MAX(CONVERT(int,v.value)),-2147483648)
               AND @pv <= ISNULL(CONVERT(int,r.value),2147483647);

            IF @pt=1 BEGIN FETCH NEXT FROM cur_pv INTO @pv; CONTINUE; END

            IF @pl=1
            BEGIN
                SET @sql='ALTER PARTITION SCHEME '+QUOTENAME(@PS)+' NEXT USED '+QUOTENAME(@Filegroup)+';'+
                         'ALTER PARTITION FUNCTION '+QUOTENAME(@PF)+'() SPLIT RANGE ('+CAST(@pv AS varchar(20))+');';
                EXEC(@sql);
            END
            ELSE
            BEGIN
                SET @TmpT=@FactTable+'_tmp_'+CAST(@pnm AS varchar(20));
                SET @sql='IF OBJECT_ID('''+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+''',''U'') IS NOT NULL DROP TABLE '+
                         QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+';'; EXEC(@sql);
                SELECT @CDef=STUFF((
                    SELECT ', '+QUOTENAME(c.name)+' '+
                           CASE WHEN y.name IN('char','nchar','binary') THEN y.name+'('+LTRIM(STR(c.max_length))+')'
                                WHEN y.name IN('varchar','nvarchar','varbinary') THEN y.name+'('+CASE WHEN c.max_length=-1 THEN 'max' ELSE LTRIM(STR(c.max_length)) END+')'
                                WHEN y.name IN('decimal','numeric') THEN y.name+'('+LTRIM(STR(c.precision))+','+LTRIM(STR(c.scale))+')'
                                ELSE y.name END+' '+CASE WHEN c.is_nullable=1 THEN 'NULL' ELSE 'NOT NULL' END
                    FROM sys.columns c JOIN sys.types y ON c.user_type_id=y.user_type_id
                    WHERE c.object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND c.is_computed=0
                    ORDER BY c.column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');
                SET @sql='CREATE TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+'('+@CDef+')'+
                         CASE WHEN @Compression IN('PAGE','ROW') THEN ' WITH (DATA_COMPRESSION='+@Compression+')' ELSE '' END+';';
                EXEC(@sql);
                SET @sql='ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                         ' SWITCH PARTITION '+CAST(@pid AS varchar(10))+
                         ' TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+';'; EXEC(@sql);
                SET @sql='ALTER PARTITION SCHEME '+QUOTENAME(@PS)+' NEXT USED '+QUOTENAME(@Filegroup)+';'+
                         'ALTER PARTITION FUNCTION '+QUOTENAME(@PF)+'() SPLIT RANGE ('+CAST(@pv AS varchar(20))+');'; EXEC(@sql);
                SELECT @pid2=sprv.boundary_id
                FROM sys.partition_functions spf
                JOIN sys.partition_range_values sprv ON sprv.function_id=spf.function_id
                WHERE spf.name=@PF AND sprv.value=@pnm;
                SET @sql='ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+
                         ' WITH CHECK ADD CONSTRAINT CK_'+@TmpT+
                         ' CHECK ('+QUOTENAME(@PartitionColumn)+' <= '+CAST(@pnm AS varchar(20))+
                         ' AND '+QUOTENAME(@PartitionColumn)+' > '+CAST(@pv AS varchar(20))+
                         ' AND '+QUOTENAME(@PartitionColumn)+' IS NOT NULL);'; EXEC(@sql);
                SET @sql='ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+
                         ' SWITCH TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                         ' PARTITION '+CAST(@pid2 AS varchar(10))+';'; EXEC(@sql);
                SET @sql='DROP TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TmpT)+';'; EXEC(@sql);
            END
            FETCH NEXT FROM cur_pv INTO @pv;
        END
        CLOSE cur_pv; DEALLOCATE cur_pv;
        DROP TABLE #hv_int;
        PRINT '=== Partition boundaries extended.';

        /* ==================================================================
           STEP 8 — Create external tables per partition
        ================================================================== */
        IF OBJECT_ID('tempdb..#plist_ext') IS NOT NULL DROP TABLE #plist_ext;
        SELECT HIGH_VALUE INTO #plist_ext
        FROM [DWH].[ext].[v_all_tab_partitions]
        WHERE TABLE_NAME=UPPER(@Verfahren) AND TABLE_OWNER=@Themengebiet;

        DECLARE @hv_ext   nvarchar(256); DECLARE @ext_tbl  nvarchar(300);
        DECLARE @cols_ext nvarchar(max); DECLARE @drop_ext nvarchar(max);
        DECLARE @crt_ext  nvarchar(max);

        DECLARE cur_ext CURSOR LOCAL FAST_FORWARD FOR
            SELECT HIGH_VALUE FROM #plist_ext;
        OPEN cur_ext; FETCH NEXT FROM cur_ext INTO @hv_ext;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @ext_tbl = LOWER(@Verfahren)+'_'+@hv_ext;
            SET @drop_ext='IF EXISTS (SELECT 1 FROM sys.external_tables WHERE schema_id=SCHEMA_ID(''ext'') AND name='''+@ext_tbl+''') '+
                          'DROP EXTERNAL TABLE ext.'+QUOTENAME(@ext_tbl)+';';
            SET @cols_ext=NULL;
            SET @sql_cols=CONCAT(N'SELECT @cols=STRING_AGG(CAST(columns_ext AS nvarchar(max)),'',''+CHAR(13)+CHAR(10))',
                                  N'WITHIN GROUP (ORDER BY colno) ',
                                  N'FROM dwh.dbo.',QUOTENAME(@TemplateTable),
                                  N' WHERE tabname=''',UPPER(@Verfahren),''' AND themengebiet=''',@Themengebiet,''';');
            EXEC sp_executesql @sql_cols, N'@cols nvarchar(max) OUTPUT', @cols=@cols_ext OUTPUT;
            SET @crt_ext=CONCAT(N'CREATE EXTERNAL TABLE ext.',QUOTENAME(@ext_tbl),N' (',CHAR(13)+CHAR(10),
                                 @cols_ext,CHAR(13)+CHAR(10),
                                 N') WITH (DATA_SOURCE=[cman-idst], LOCATION=''ESTAT.',@Themengebiet,'.',UPPER(@Verfahren),''');');
            EXEC(@drop_ext); EXEC(@crt_ext);
            FETCH NEXT FROM cur_ext INTO @hv_ext;
        END
        CLOSE cur_ext; DEALLOCATE cur_ext;
        DROP TABLE #plist_ext;
        PRINT '=== External tables created.';

        /* ==================================================================
           STEP 9 — Create staging _in / _out tables
        ================================================================== */
        IF OBJECT_ID('tempdb..#plist_stg') IS NOT NULL DROP TABLE #plist_stg;
        SELECT HIGH_VALUE INTO #plist_stg
        FROM [DWH].[ext].[v_all_tab_partitions]
        WHERE TABLE_NAME=UPPER(@Verfahren) AND TABLE_OWNER=@Themengebiet;

        DECLARE @hv_stg   nvarchar(256); DECLARE @stg_in   nvarchar(300);
        DECLARE @stg_out  nvarchar(300); DECLARE @cols_stg  nvarchar(max);

        DECLARE cur_stg CURSOR LOCAL FAST_FORWARD FOR
            SELECT HIGH_VALUE FROM #plist_stg;
        OPEN cur_stg; FETCH NEXT FROM cur_stg INTO @hv_stg;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @stg_in  = LOWER(@Verfahren)+'_in_' +@hv_stg;
            SET @stg_out = LOWER(@Verfahren)+'_out_'+@hv_stg;
            SET @cols_stg=NULL;
            SET @sql_cols=CONCAT(N'SELECT @cols=STRING_AGG(CAST(columns_ext AS nvarchar(max)),'',''+CHAR(13)+CHAR(10))',
                                  N'WITHIN GROUP (ORDER BY colno) ',
                                  N'FROM dwh.dbo.',QUOTENAME(@TemplateTable),
                                  N' WHERE tabname=''',UPPER(@Verfahren),''' AND themengebiet=''',@Themengebiet,''';');
            EXEC sp_executesql @sql_cols, N'@cols nvarchar(max) OUTPUT', @cols=@cols_stg OUTPUT;

            SET @sql='IF EXISTS (SELECT 1 FROM sys.tables WHERE schema_id=SCHEMA_ID(''dbo'') AND name='''+@stg_in+''') DROP TABLE dbo.['+@stg_in+'];';
            EXEC(@sql);
            SET @sql='CREATE TABLE dbo.['+@stg_in+'] ('+CHAR(13)+CHAR(10)+@cols_stg+CHAR(13)+CHAR(10)+');';
            EXEC(@sql);

            SET @sql='IF EXISTS (SELECT 1 FROM sys.tables WHERE schema_id=SCHEMA_ID(''dbo'') AND name='''+@stg_out+''') DROP TABLE dbo.['+@stg_out+'];';
            EXEC(@sql);
            SET @sql='CREATE TABLE dbo.['+@stg_out+'] ('+CHAR(13)+CHAR(10)+@cols_stg+CHAR(13)+CHAR(10)+');';
            EXEC(@sql);

            FETCH NEXT FROM cur_stg INTO @hv_stg;
        END
        CLOSE cur_stg; DEALLOCATE cur_stg;
        DROP TABLE #plist_stg;
        PRINT '=== Staging _in/_out tables created.';

        /* ==================================================================
           STEP 10 — Indexes on _in / _out tables
        ================================================================== */
        DECLARE @idx_tbl nvarchar(300); DECLARE @CIC_idx nvarchar(max);
        DECLARE @CL2     nvarchar(max);

        IF @IndexType='TRUE'
        BEGIN
            SELECT @CIC_idx=STUFF((
                SELECT ', '+QUOTENAME(c.name)
                FROM sys.index_columns ic
                JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
                WHERE ic.object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)) AND ic.index_id=1
                ORDER BY ic.key_ordinal FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');
            IF @CIC_idx IS NULL SET @CIC_idx=QUOTENAME(@PartitionColumn);
        END

        DECLARE cur_idx CURSOR LOCAL FAST_FORWARD FOR
            SELECT name FROM sys.tables
            WHERE schema_id=SCHEMA_ID(@SchemaName)
              AND (name LIKE LOWER(@FactTable)+'_in_%' OR name LIKE LOWER(@FactTable)+'_out_%')
            ORDER BY name;
        OPEN cur_idx; FETCH NEXT FROM cur_idx INTO @idx_tbl;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @IndexType='TRUE' AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)) AND name='CI_'+@idx_tbl)
            BEGIN
                SET @sql='CREATE CLUSTERED INDEX '+QUOTENAME('CI_'+@idx_tbl)+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)+
                         ' ('+@CIC_idx+') WITH (FILLFACTOR=100,SORT_IN_TEMPDB=ON);';
                EXEC(@sql);
            END
            ELSE IF @IndexType='CCI' AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)) AND name='CCI_'+@idx_tbl)
            BEGIN
                SET @sql='CREATE CLUSTERED COLUMNSTORE INDEX '+QUOTENAME('CCI_'+@idx_tbl)+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)+';';
                EXEC(@sql);
            END
            IF @idx_tbl LIKE LOWER(@FactTable)+'_out_%' AND @NcciFlag='TRUE' AND @IndexType!='CCI'
               AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)) AND name='NCCI_'+@idx_tbl)
            BEGIN
                SELECT @CL2=STRING_AGG(QUOTENAME(name),',') FROM sys.columns
                WHERE object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)) AND is_computed=0;
                SET @sql='CREATE NONCLUSTERED COLUMNSTORE INDEX '+QUOTENAME('NCCI_'+@idx_tbl)+' ON '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@idx_tbl)+' ('+@CL2+');';
                EXEC(@sql);
            END
            FETCH NEXT FROM cur_idx INTO @idx_tbl;
        END
        CLOSE cur_idx; DEALLOCATE cur_idx;
        PRINT '=== Indexes created on _in/_out.';

        /* ==================================================================
           STEP 11 — DATA COPY: ext → _in  (with WHERE clause from control table)
           Simple SELECT * INTO _in FROM ext.table WHERE <where_klausel>
           WHERE clause is pushed down to Oracle via PolyBase predicate pushdown
        ================================================================== */
        IF OBJECT_ID('tempdb..#plist_load') IS NOT NULL DROP TABLE #plist_load;
        SELECT HIGH_VALUE INTO #plist_load
        FROM [DWH].[ext].[v_all_tab_partitions]
        WHERE TABLE_NAME=UPPER(@Verfahren) AND TABLE_OWNER=@Themengebiet;

        DECLARE @hv_load   nvarchar(256);
        DECLARE @ext_load  nvarchar(300);
        DECLARE @in_load   nvarchar(300);
        DECLARE @rows_part bigint;
        SET @rows_loaded = 0;

        DECLARE cur_load CURSOR LOCAL FAST_FORWARD FOR
            SELECT HIGH_VALUE FROM #plist_load ORDER BY HIGH_VALUE;
        OPEN cur_load; FETCH NEXT FROM cur_load INTO @hv_load;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @ext_load = LOWER(@Verfahren)+'_'+@hv_load;
            SET @in_load  = LOWER(@Verfahren)+'_in_'+@hv_load;

            PRINT '    Loading partition: ' + @hv_load;

            -- Check ext table exists
            IF NOT EXISTS (SELECT 1 FROM sys.external_tables
                           WHERE schema_id=SCHEMA_ID(@ExtSchema) AND name=@ext_load)
            BEGIN
                PRINT '    WARN: ext table not found → skip: ' + @ext_load;
                FETCH NEXT FROM cur_load INTO @hv_load;
                CONTINUE;
            END

            -- Check _in table exists
            IF NOT EXISTS (SELECT 1 FROM sys.tables
                           WHERE schema_id=SCHEMA_ID(@SchemaName) AND name=@in_load)
            BEGIN
                PRINT '    WARN: _in table not found → skip: ' + @in_load;
                FETCH NEXT FROM cur_load INTO @hv_load;
                CONTINUE;
            END

            -- Truncate _in before loading (idempotent re-run)
            SET @sql = 'TRUNCATE TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@in_load)+';';
            EXEC(@sql);

            -- ----------------------------------------------------------------
            --  THE DATA COPY
            --  INSERT INTO dbo.[table_in_HV]
            --  SELECT * FROM ext.[table_HV]
            --  WHERE <where_klausel resolved from tabname_filter>
            --
            --  If where_klausel is NULL (Case 7 / full load) → no WHERE clause
            --  The WHERE is pushed down to Oracle via PolyBase
            -- ----------------------------------------------------------------
            SET @sql =
                'INSERT INTO ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@in_load) + CHAR(13)+CHAR(10) +
                'SELECT *' + CHAR(13)+CHAR(10) +
                'FROM   ' + QUOTENAME(@ExtSchema) + '.' + QUOTENAME(@ext_load) +
                CASE
                    WHEN @where_klausel IS NOT NULL
                    THEN CHAR(13)+CHAR(10) + @where_klausel   -- e.g. WHERE MON_ID = '202410'
                    ELSE ''                                     -- full load, no filter
                END + ';';

            PRINT '    SQL: ' + @sql;

            BEGIN TRY
                EXEC(@sql);
                SET @rows_part = @@ROWCOUNT;
                SET @rows_loaded = @rows_loaded + @rows_part;
                PRINT '    Rows inserted: ' + CAST(@rows_part AS nvarchar(20));
            END TRY
            BEGIN CATCH
                PRINT '    ERROR loading partition ' + @hv_load + ': ' + ERROR_MESSAGE();
                THROW;
            END CATCH;

            FETCH NEXT FROM cur_load INTO @hv_load;
        END
        CLOSE cur_load; DEALLOCATE cur_load;
        DROP TABLE #plist_load;
        PRINT '=== Data copy complete. Total rows loaded: ' + CAST(@rows_loaded AS nvarchar(20));

        /* ==================================================================
           STEP 12 — PARTITION SWITCH: _in → FactTable  (Schritt 9 logic)
           1. SWITCH OUT FactTable partition → _out  (clear old data)
           2. ADD CHECK constraint on _in
           3. SWITCH IN _in → FactTable partition
           4. DROP _out and _in
        ================================================================== */
        DECLARE @sw_in    nvarchar(300);
        DECLARE @sw_out   nvarchar(300);
        DECLARE @sw_hv    nvarchar(20);
        DECLARE @sw_pnr   int;
        DECLARE @sw_ck    nvarchar(300);

        DECLARE cur_sw CURSOR LOCAL FAST_FORWARD FOR
            SELECT name FROM sys.tables
            WHERE schema_id=SCHEMA_ID(@SchemaName)
              AND name LIKE LOWER(@FactTable)+'_in_%'
            ORDER BY name;

        OPEN cur_sw; FETCH NEXT FROM cur_sw INTO @sw_in;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Extract HIGH_VALUE from table name
            SET @sw_hv  = REPLACE(@sw_in, LOWER(@FactTable)+'_in_', '');
            SET @sw_out = LOWER(@FactTable)+'_out_'+@sw_hv;

            PRINT '    Switch partition: ' + @sw_hv;

            -- Find partition number (boundary_id)
            SELECT @sw_pnr = sprv.boundary_id
            FROM sys.partition_functions spf
            JOIN sys.partition_range_values sprv ON sprv.function_id=spf.function_id
            WHERE spf.name=@PF AND sprv.value=CONVERT(int,@sw_hv);

            IF @sw_pnr IS NULL OR @sw_pnr = 0
            BEGIN
                PRINT '    WARN: partition not found for value: ' + @sw_hv + ' → skip';
                FETCH NEXT FROM cur_sw INTO @sw_in;
                CONTINUE;
            END

            -- 12a: SWITCH OUT FactTable → _out  (move existing data out)
            SET @sql =
                'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                ' SWITCH PARTITION '+CAST(@sw_pnr AS varchar(10))+
                ' TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_out)+';';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    SWITCH OUT → ' + @sw_out;
            END TRY
            BEGIN CATCH
                PRINT '    ERROR SWITCH OUT: ' + ERROR_MESSAGE(); THROW;
            END CATCH;

            -- 12b: ADD CHECK constraint on _in
            --      Required by SQL Server to allow SWITCH IN
            SET @sw_ck = @PartitionColumn+'_'+@sw_hv+'_'+@FactTable+'_CK';

            IF EXISTS (SELECT 1 FROM sys.check_constraints
                       WHERE parent_object_id=OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in))
                         AND name=@sw_ck)
            BEGIN
                SET @sql='ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in)+
                         ' DROP CONSTRAINT '+QUOTENAME(@sw_ck)+';';
                EXEC(@sql);
            END

            SET @sql =
                'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in)+
                ' ADD CONSTRAINT '+QUOTENAME(@sw_ck)+
                ' CHECK ('+QUOTENAME(@PartitionColumn)+' = '+@sw_hv+');';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    CHECK constraint set: ' + @sw_ck;
            END TRY
            BEGIN CATCH
                PRINT '    ERROR CHECK constraint: ' + ERROR_MESSAGE(); THROW;
            END CATCH;

            -- 12c: SWITCH IN _in → FactTable
            SET @sql =
                'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in)+
                ' SWITCH TO '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@FactTable)+
                ' PARTITION '+CAST(@sw_pnr AS varchar(10))+';';
            BEGIN TRY
                EXEC(@sql);
                PRINT '    SWITCH IN → ' + @FactTable + ' partition ' + CAST(@sw_pnr AS nvarchar(10));
            END TRY
            BEGIN CATCH
                PRINT '    ERROR SWITCH IN: ' + ERROR_MESSAGE(); THROW;
            END CATCH;

            -- 12d: Cleanup _out and _in
            IF OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_out),'U') IS NOT NULL
            BEGIN
                SET @sql='DROP TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_out)+';';
                BEGIN TRY EXEC(@sql); PRINT '    Dropped: '+@sw_out;
                END TRY BEGIN CATCH PRINT '    WARN drop _out: '+ERROR_MESSAGE(); END CATCH;
            END

            IF OBJECT_ID(QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in),'U') IS NOT NULL
            BEGIN
                SET @sql='DROP TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@sw_in)+';';
                BEGIN TRY EXEC(@sql); PRINT '    Dropped: '+@sw_in;
                END TRY BEGIN CATCH PRINT '    WARN drop _in: '+ERROR_MESSAGE(); END CATCH;
            END

            PRINT '    Partition done: ' + @sw_hv;
            FETCH NEXT FROM cur_sw INTO @sw_in;
        END
        CLOSE cur_sw; DEALLOCATE cur_sw;
        PRINT '=== Partition switch complete for: ' + @FactTable;

        /* ==================================================================
           SUCCESS
        ================================================================== */
        SET @Status    = 'OK';
        SET @Bemerkung = 'WHERE: ' + ISNULL(@where_klausel,'NULL (full load)') +
                         ' | Rows loaded: ' + CAST(@rows_loaded AS nvarchar(20));
        SET @cnt_ok   += 1;
        PRINT '=== STATUS: OK | Rows: ' + CAST(@rows_loaded AS nvarchar(20));

    END TRY
    BEGIN CATCH
        SET @ErrMsg    = ERROR_MESSAGE();
        SET @Status    = 'FAILED';
        SET @Bemerkung = LEFT(@ErrMsg, 500);
        SET @cnt_failed += 1;
        PRINT '=== STATUS: FAILED — ' + @ErrMsg;
    END CATCH

    INSERT INTO #summary VALUES (
        @Verfahren, @Themengebiet, @tabname_filter,
        @where_klausel, @rows_loaded,
        @Status, @Bemerkung, @refDatum
    );

    FETCH NEXT FROM cur_main INTO @Verfahren, @Themengebiet, @tabname_filter, @FileName;
END

CLOSE cur_main; DEALLOCATE cur_main;
DROP TABLE #control;

-- ============================================================================
--  PRINT SUMMARY
-- ============================================================================
PRINT '';
PRINT '════════════════════════════════════════════════════════';
PRINT 'FINAL SUMMARY';
PRINT '════════════════════════════════════════════════════════';
PRINT 'Total in tm_steuerlistenfile_Fakten : ' + CAST(@cnt_total     AS nvarchar(20));
PRINT 'Processed OK                        : ' + CAST(@cnt_ok        AS nvarchar(20));
PRINT 'Object not found                    : ' + CAST(@cnt_not_found AS nvarchar(20));
PRINT 'Failed                              : ' + CAST(@cnt_failed    AS nvarchar(20));
PRINT '════════════════════════════════════════════════════════';

-- ============================================================================
--  CSV OUTPUT — one row per entry + counter columns
-- ============================================================================
SELECT
    Verfahren,
    Themengebiet,
    tabname_filter,
    ISNULL(where_klausel,'NULL (full load)')    AS where_klausel,
    rows_loaded,
    Status,
    Bemerkung,
    FORMAT(ref_datum,'yyyy-MM-dd HH:mm:ss')     AS ref_datum,
    @cnt_total      AS total_in_control,
    @cnt_ok         AS processed_ok,
    @cnt_not_found  AS object_not_found,
    @cnt_failed     AS failed
FROM #summary
ORDER BY Status DESC, Verfahren, tabname_filter;

-- Totals row
SELECT
    'TOTALS' AS Verfahren, '' AS Themengebiet,
    '' AS tabname_filter,  '' AS where_klausel,
    SUM(rows_loaded) AS rows_loaded,
    '' AS Status, '' AS Bemerkung, '' AS ref_datum,
    @cnt_total     AS total_in_control,
    @cnt_ok        AS processed_ok,
    @cnt_not_found AS object_not_found,
    @cnt_failed    AS failed
FROM #summary;

DROP TABLE #summary;
