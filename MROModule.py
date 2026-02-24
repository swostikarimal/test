def getPks(jdbc_url, connection_properties, spark=None):
    '''
    '''
    pk_query = """
    (
        SELECT  
            t.name AS table_name,
            i.name AS pk_name,
            STRING_AGG(c.name, ',') AS pk_columns,
            COUNT(*) AS column_count,
            CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS is_composite
        FROM sys.tables t
        INNER JOIN sys.indexes i
            ON t.object_id = i.object_id AND i.is_primary_key = 1
        INNER JOIN sys.index_columns ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        GROUP BY t.name, i.name
    ) pk_summary

    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=pk_query,
        properties=connection_properties
    )

    return df

def getFks(jdbc_url, connection_properties, spark=None):
    '''
    '''
    fk_query = """
    (
        SELECT  
            tp.name AS table_with_fk,
            tr.name AS references_table,
            COUNT(*) AS num_foreign_keys,
            STRING_AGG(CONCAT(cp.name, ' -> ', cr.name), ', ') AS column_mappings
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc 
            ON fkc.constraint_object_id = fk.object_id
        INNER JOIN sys.tables tp 
            ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns cp 
            ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables tr 
            ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns cr 
            ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        GROUP BY tp.name, tr.name
    )   fk_summary
    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=fk_query,
        properties=connection_properties
    )

    return df